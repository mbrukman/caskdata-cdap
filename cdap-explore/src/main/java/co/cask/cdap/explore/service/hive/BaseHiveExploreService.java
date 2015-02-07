/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.explore.service.hive;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.explore.service.MetaDataInfo;
import co.cask.cdap.explore.service.TableNotFoundException;
import co.cask.cdap.hive.context.CConfCodec;
import co.cask.cdap.hive.context.ConfigurationUtil;
import co.cask.cdap.hive.context.ContextManager;
import co.cask.cdap.hive.context.HConfCodec;
import co.cask.cdap.hive.context.TxnCodec;
import co.cask.cdap.hive.datasets.DatasetAccessor;
import co.cask.cdap.hive.datasets.DatasetStorageHandler;
import co.cask.cdap.hive.stream.StreamStorageHandler;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryInfo;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.TableInfo;
import co.cask.cdap.proto.TableNameInfo;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.thrift.TColumnValue;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.thrift.TException;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;

/**
 * Defines common functionality used by different HiveExploreServices. The common functionality includes
 * starting/stopping transactions, serializing configuration and saving operation information.
 *
 * Overridden {@link co.cask.cdap.explore.service.Explore} methods also call {@code startAndWait()},
 * which effectively allows this {@link com.google.common.util.concurrent.Service} to not have to start
 * until the first call to the explore methods is made. This is used for {@link Constants.Explore#START_ON_DEMAND},
 * which, if true, does not start the {@link ExploreService} when the explore HTTP services are started.
 */
public abstract class BaseHiveExploreService extends AbstractIdleService implements ExploreService {
  private static final Logger LOG = LoggerFactory.getLogger(BaseHiveExploreService.class);
  private static final Gson GSON = new Gson();
  private static final int PREVIEW_COUNT = 5;
  private static final long METASTORE_CLIENT_CLEANUP_PERIOD = 60;

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final HiveConf hiveConf;
  private final TransactionSystemClient txClient;

  // Handles that are running, or not yet completely fetched, they have longer timeout
  private final Cache<QueryHandle, OperationInfo> activeHandleCache;
  // Handles that don't have any more results to be fetched, they can be timed out aggressively.
  private final Cache<QueryHandle, InactiveOperationInfo> inactiveHandleCache;

  private final CLIService cliService;
  private final ScheduledExecutorService scheduledExecutorService;
  private final long cleanupJobSchedule;
  private final File previewsDir;
  private final ScheduledExecutorService metastoreClientsExecutorService;

  private final ThreadLocal<Supplier<IMetaStoreClient>> metastoreClientLocal;

  // The following two fields are for tracking GC'ed metastore clients and be able to call close on them.
  private final Map<Reference<? extends Supplier<IMetaStoreClient>>, IMetaStoreClient> metastoreClientReferences;
  private final ReferenceQueue<Supplier<IMetaStoreClient>> metastoreClientReferenceQueue;

  protected abstract QueryStatus fetchStatus(OperationHandle handle) throws HiveSQLException, ExploreException,
    HandleNotFoundException;
  protected abstract OperationHandle doExecute(SessionHandle sessionHandle, String statement)
    throws HiveSQLException, ExploreException;

  protected BaseHiveExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                                   CConfiguration cConf, Configuration hConf, HiveConf hiveConf,
                                   File previewsDir, StreamAdmin streamAdmin) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.hiveConf = hiveConf;
    this.previewsDir = previewsDir;
    this.metastoreClientLocal = new ThreadLocal<Supplier<IMetaStoreClient>>();
    this.metastoreClientReferences = Maps.newConcurrentMap();
    this.metastoreClientReferenceQueue = new ReferenceQueue<Supplier<IMetaStoreClient>>();

    // Create a Timer thread to periodically collect metastore clients that are no longer in used and call close on them
    this.metastoreClientsExecutorService =
      Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("metastore-client-gc"));

    this.scheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("explore-handle-timeout"));

    this.activeHandleCache =
      CacheBuilder.newBuilder()
        .expireAfterWrite(cConf.getLong(Constants.Explore.ACTIVE_OPERATION_TIMEOUT_SECS), TimeUnit.SECONDS)
        .removalListener(new ActiveOperationRemovalHandler(this, scheduledExecutorService))
        .build();
    this.inactiveHandleCache =
      CacheBuilder.newBuilder()
        .expireAfterWrite(cConf.getLong(Constants.Explore.INACTIVE_OPERATION_TIMEOUT_SECS), TimeUnit.SECONDS)
        .build();

    this.cliService = new CLIService();

    this.txClient = txClient;
    ContextManager.saveContext(datasetFramework, streamAdmin);

    cleanupJobSchedule = cConf.getLong(Constants.Explore.CLEANUP_JOB_SCHEDULE_SECS);

    LOG.info("Active handle timeout = {} secs", cConf.getLong(Constants.Explore.ACTIVE_OPERATION_TIMEOUT_SECS));
    LOG.info("Inactive handle timeout = {} secs", cConf.getLong(Constants.Explore.INACTIVE_OPERATION_TIMEOUT_SECS));
    LOG.info("Cleanup job schedule = {} secs", cleanupJobSchedule);
  }

  protected HiveConf getHiveConf() {
    return new HiveConf();
  }

  protected CLIService getCliService() {
    return cliService;
  }

  private IMetaStoreClient getMetaStoreClient() throws ExploreException {
    if (metastoreClientLocal.get() == null) {
      try {
        IMetaStoreClient client = new HiveMetaStoreClient(new HiveConf());
        Supplier<IMetaStoreClient> supplier = Suppliers.ofInstance(client);
        metastoreClientLocal.set(supplier);

        // We use GC of the supplier as a signal for us to know that a thread is gone
        // The supplier is set into the thread local, which will get GC'ed when the thread is gone.
        // Since we use a weak reference key to the supplier that points to the client
        // (in the metastoreClientReferences map), it won't block GC of the supplier instance.
        // We can use the weak reference, which is retrieved through polling the ReferenceQueue,
        // to get back the client and call close() on it.
        metastoreClientReferences.put(
          new WeakReference<Supplier<IMetaStoreClient>>(supplier, metastoreClientReferenceQueue),
          client
        );
      } catch (MetaException e) {
        throw new ExploreException("Error initializing Hive Metastore client", e);
      }
    }
    return metastoreClientLocal.get().get();
  }

  private void closeMetastoreClient(IMetaStoreClient client) {
    try {
      client.close();
    } catch (Throwable t) {
      LOG.error("Exception raised in closing Metastore client", t);
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}...", BaseHiveExploreService.class.getSimpleName());
    cliService.init(getHiveConf());
    cliService.start();

    metastoreClientsExecutorService.scheduleWithFixedDelay(
      new Runnable() {
        @Override
        public void run() {
          Reference<? extends Supplier<IMetaStoreClient>> ref = metastoreClientReferenceQueue.poll();
          while (ref != null) {
            IMetaStoreClient client = metastoreClientReferences.remove(ref);
            if (client != null) {
              closeMetastoreClient(client);
            }
            ref = metastoreClientReferenceQueue.poll();
          }
        }
      },
      METASTORE_CLIENT_CLEANUP_PERIOD, METASTORE_CLIENT_CLEANUP_PERIOD, TimeUnit.SECONDS);

    // Schedule the cache cleanup
    scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
                                                      @Override
                                                      public void run() {
                                                        runCacheCleanup();
                                                      }
                                                    }, cleanupJobSchedule, cleanupJobSchedule, TimeUnit.SECONDS
    );
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}...", BaseHiveExploreService.class.getSimpleName());

    // By this time we should not get anymore new requests, since HTTP service has already been stopped.
    // Close all handles
    if (!activeHandleCache.asMap().isEmpty()) {
      LOG.info("Timing out active handles...");
    }
    activeHandleCache.invalidateAll();
    // Make sure the cache entries get expired.
    runCacheCleanup();

    // Wait for all cleanup jobs to complete
    scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS);
    scheduledExecutorService.shutdown();

    metastoreClientsExecutorService.shutdownNow();
    // Go through all non-cleanup'ed clients and call close() upon them
    for (IMetaStoreClient client : metastoreClientReferences.values()) {
      closeMetastoreClient(client);
    }

    cliService.stop();
    
    // Close all resources associated with instantiated Datasets
    DatasetAccessor.closeAllQueries();
  }

  @Override
  public QueryHandle getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
    throws ExploreException, SQLException {
    startAndWait();

    try {
      Map<String, String> sessionConf = startSession();
      SessionHandle sessionHandle = cliService.openSession("", "", sessionConf);
      try {
        OperationHandle operationHandle = cliService.getColumns(sessionHandle, catalog, schemaPattern,
                                                                tableNamePattern, columnNamePattern);
        QueryHandle handle = saveOperationInfo(operationHandle, sessionHandle, sessionConf, "");
        LOG.trace("Retrieving columns: catalog {}, schemaPattern {}, tableNamePattern {}, columnNamePattern {}",
                  catalog, schemaPattern, tableNamePattern, columnNamePattern);
        return handle;
      } catch (Throwable e) {
        closeSession(sessionHandle);
        throw e;
      }
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    } catch (Throwable e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public QueryHandle getCatalogs() throws ExploreException, SQLException {
    startAndWait();

    try {
      Map<String, String> sessionConf = startSession();
      SessionHandle sessionHandle = cliService.openSession("", "", sessionConf);
      try {
        OperationHandle operationHandle = cliService.getCatalogs(sessionHandle);
        QueryHandle handle = saveOperationInfo(operationHandle, sessionHandle, sessionConf, "");
        LOG.trace("Retrieving catalogs");
        return handle;
      } catch (Throwable e) {
        closeSession(sessionHandle);
        throw e;
      }
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    } catch (Throwable e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public QueryHandle getSchemas(String catalog, String schemaPattern) throws ExploreException, SQLException {
    startAndWait();

    try {
      Map<String, String> sessionConf = startSession();
      SessionHandle sessionHandle = cliService.openSession("", "", sessionConf);
      try {
        OperationHandle operationHandle = cliService.getSchemas(sessionHandle, catalog, schemaPattern);
        QueryHandle handle = saveOperationInfo(operationHandle, sessionHandle, sessionConf, "");
        LOG.trace("Retrieving schemas: catalog {}, schema {}", catalog, schemaPattern);
        return handle;
      } catch (Throwable e) {
        closeSession(sessionHandle);
        throw e;
      }
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    } catch (Throwable e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public QueryHandle getFunctions(String catalog, String schemaPattern, String functionNamePattern)
    throws ExploreException, SQLException {
    startAndWait();

    try {
      Map<String, String> sessionConf = startSession();
      SessionHandle sessionHandle = cliService.openSession("", "", sessionConf);
      try {
        OperationHandle operationHandle = cliService.getFunctions(sessionHandle, catalog,
                                                                  schemaPattern, functionNamePattern);
        QueryHandle handle = saveOperationInfo(operationHandle, sessionHandle, sessionConf, "");
        LOG.trace("Retrieving functions: catalog {}, schema {}, function {}",
                  catalog, schemaPattern, functionNamePattern);
        return handle;
      } catch (Throwable e) {
        closeSession(sessionHandle);
        throw e;
      }
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    } catch (Throwable e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public MetaDataInfo getInfo(MetaDataInfo.InfoType infoType) throws ExploreException, SQLException {
    startAndWait();

    try {
      MetaDataInfo ret = infoType.getDefaultValue();
      if (ret != null) {
        return ret;
      }

      Map<String, String> sessionConf = startSession();
      SessionHandle sessionHandle = cliService.openSession("", "", sessionConf);
      try {
        // Convert to GetInfoType
        GetInfoType hiveInfoType = null;
        for (GetInfoType t : GetInfoType.values()) {
          if (t.name().equals("CLI_" + infoType.name())) {
            hiveInfoType = t;
            break;
          }
        }
        if (hiveInfoType == null) {
          // Should not come here, unless there is a mismatch between Explore and Hive info types.
          LOG.warn("Could not find Hive info type %s", infoType);
          return null;
        }
        GetInfoValue val = cliService.getInfo(sessionHandle, hiveInfoType);
        LOG.trace("Retrieving info: {}, got value {}", infoType, val);
        return new MetaDataInfo(val.getStringValue(), val.getShortValue(), val.getIntValue(), val.getLongValue());
      } finally {
        closeSession(sessionHandle);
      }
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    } catch (IOException e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public QueryHandle getTables(String catalog, String schemaPattern, String tableNamePattern,
                               List<String> tableTypes) throws ExploreException, SQLException {
    startAndWait();

    try {
      Map<String, String> sessionConf = startSession();
      SessionHandle sessionHandle = cliService.openSession("", "", sessionConf);
      try {
        OperationHandle operationHandle = cliService.getTables(sessionHandle, catalog, schemaPattern,
                                                               tableNamePattern, tableTypes);
        QueryHandle handle = saveOperationInfo(operationHandle, sessionHandle, sessionConf, "");
        LOG.trace("Retrieving tables: catalog {}, schemaNamePattern {}, tableNamePattern {}, tableTypes {}",
                  catalog, schemaPattern, tableNamePattern, tableTypes);
        return handle;
      } catch (Throwable e) {
        closeSession(sessionHandle);
        throw e;
      }
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    } catch (Throwable e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public List<TableNameInfo> getTables(@Nullable final String database) throws ExploreException {
    startAndWait();

    // TODO check if the database user is allowed to access if security is enabled and
    // namespacing is in place.

    try {
      List<String> databases;
      if (database == null) {
        databases = getMetaStoreClient().getAllDatabases();
      } else {
        databases = ImmutableList.of(database);
      }
      ImmutableList.Builder<TableNameInfo> builder = ImmutableList.builder();
      for (String db : databases) {
        List<String> tables = getMetaStoreClient().getAllTables(db);
        for (String table : tables) {
          builder.add(new TableNameInfo(db, table));
        }
      }
      return builder.build();
    } catch (TException e) {
      throw new ExploreException("Error connecting to Hive metastore", e);
    }
  }

  @Override
  public TableInfo getTableInfo(@Nullable String database, String table)
    throws ExploreException, TableNotFoundException {
    startAndWait();

    // TODO check if the database user is allowed to access if security is enabled and
    // namespacing is in place.

    try {
      String db = database == null ? "default" : database;

      Table tableInfo = getMetaStoreClient().getTable(db, table);
      List<FieldSchema> tableFields = tableInfo.getSd().getCols();
      // for whatever reason, it seems like the table columns for partitioned tables are not present
      // in the storage descriptor. If columns are missing, do a separate call for schema.
      if (tableFields == null || tableFields.isEmpty()) {
        // don't call .getSchema()... class not found exception if we do in the thrift code...
        tableFields = getMetaStoreClient().getFields(db, table);
      }

      ImmutableList.Builder<TableInfo.ColumnInfo> schemaBuilder = ImmutableList.builder();
      Set<String> fieldNames = Sets.newHashSet();
      for (FieldSchema column : tableFields) {
        schemaBuilder.add(new TableInfo.ColumnInfo(column.getName(), column.getType(), column.getComment()));
        fieldNames.add(column.getName());
      }

      ImmutableList.Builder<TableInfo.ColumnInfo> partitionKeysBuilder = ImmutableList.builder();
      for (FieldSchema column : tableInfo.getPartitionKeys()) {
        TableInfo.ColumnInfo columnInfo = new TableInfo.ColumnInfo(column.getName(), column.getType(),
                                                                   column.getComment());
        partitionKeysBuilder.add(columnInfo);
        // add partition keys to the schema if they are not already there,
        // since they show up when you do a 'describe <table>' command.
        if (!fieldNames.contains(column.getName())) {
          schemaBuilder.add(columnInfo);
        }
      }

      // its a cdap generated table if it uses our storage handler, or if a property is set on the table.
      String cdapName = null;
      Map<String, String> tableParameters = tableInfo.getParameters();
      if (tableParameters != null) {
        cdapName = tableParameters.get(Constants.Explore.CDAP_NAME);
      }
      // tables created after CDAP 2.6 should set the "cdap.name" property, but older ones
      // do not. So also check if it uses a cdap storage handler.
      String storageHandler = tableInfo.getParameters().get("storage_handler");
      boolean isDatasetTable = cdapName != null ||
        DatasetStorageHandler.class.getName().equals(storageHandler) ||
        StreamStorageHandler.class.getName().equals(storageHandler);

      return new TableInfo(tableInfo.getTableName(), tableInfo.getDbName(), tableInfo.getOwner(),
                           (long) tableInfo.getCreateTime() * 1000, (long) tableInfo.getLastAccessTime() * 1000,
                           tableInfo.getRetention(), partitionKeysBuilder.build(), tableInfo.getParameters(),
                           tableInfo.getTableType(), schemaBuilder.build(), tableInfo.getSd().getLocation(),
                           tableInfo.getSd().getInputFormat(), tableInfo.getSd().getOutputFormat(),
                           tableInfo.getSd().isCompressed(), tableInfo.getSd().getNumBuckets(),
                           tableInfo.getSd().getSerdeInfo().getSerializationLib(),
                           tableInfo.getSd().getSerdeInfo().getParameters(), isDatasetTable);
    } catch (NoSuchObjectException e) {
      throw new TableNotFoundException(e);
    } catch (TException e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public QueryHandle getTableTypes() throws ExploreException, SQLException {
    startAndWait();

    try {
      Map<String, String> sessionConf = startSession();
      SessionHandle sessionHandle = cliService.openSession("", "", sessionConf);
      try {
        OperationHandle operationHandle = cliService.getTableTypes(sessionHandle);
        QueryHandle handle = saveOperationInfo(operationHandle, sessionHandle, sessionConf, "");
        LOG.trace("Retrieving table types");
        return handle;
      } catch (Throwable e) {
        closeSession(sessionHandle);
        throw e;
      }
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    } catch (Throwable e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public QueryHandle getTypeInfo() throws ExploreException, SQLException {
    startAndWait();

    try {
      Map<String, String> sessionConf = startSession();
      SessionHandle sessionHandle = cliService.openSession("", "", sessionConf);
      try {
        OperationHandle operationHandle = cliService.getTypeInfo(sessionHandle);
        QueryHandle handle = saveOperationInfo(operationHandle, sessionHandle, sessionConf, "");
        LOG.trace("Retrieving type info");
        return handle;
      } catch (Throwable e) {
        closeSession(sessionHandle);
        throw e;
      }
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    } catch (Throwable e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public QueryHandle execute(String statement) throws ExploreException, SQLException {
    startAndWait();

    try {
      Map<String, String> sessionConf = startSession();
      // It looks like the username and password below is not used when security is disabled in Hive Server2.
      SessionHandle sessionHandle = cliService.openSession("", "", sessionConf);
      try {
        OperationHandle operationHandle = doExecute(sessionHandle, statement);
        QueryHandle handle = saveOperationInfo(operationHandle, sessionHandle, sessionConf, statement);
        LOG.trace("Executing statement: {} with handle {}", statement, handle);
        return handle;
      } catch (Throwable e) {
        closeSession(sessionHandle);
        throw e;
      }
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    } catch (Throwable e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public QueryStatus getStatus(QueryHandle handle) throws ExploreException, HandleNotFoundException, SQLException {
    startAndWait();

    InactiveOperationInfo inactiveOperationInfo = inactiveHandleCache.getIfPresent(handle);
    if (inactiveOperationInfo != null) {
      // Operation has been made inactive, so return the saved status.
      LOG.trace("Returning saved status for inactive handle {}", handle);
      return inactiveOperationInfo.getStatus();
    }

    try {
      // Fetch status from Hive
      QueryStatus status = fetchStatus(getOperationHandle(handle));
      LOG.trace("Status of handle {} is {}", handle, status);

      // No results or error, so can be timed out aggressively
      if (status.getStatus() == QueryStatus.OpStatus.FINISHED && !status.hasResults()) {
        // In case of a query that writes to a Dataset, we will always fall into this condition,
        // and timing out aggressively will also close the transaction and make the writes visible
        timeoutAggresively(handle, getResultSchema(handle), status);
      } else if (status.getStatus() == QueryStatus.OpStatus.ERROR) {
        // getResultSchema will fail if the query is in error
        timeoutAggresively(handle, ImmutableList.<ColumnDesc>of(), status);
      }
      return status;
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    }
  }

  @Override
  public List<QueryResult> nextResults(QueryHandle handle, int size)
    throws ExploreException, HandleNotFoundException, SQLException {
    startAndWait();

    InactiveOperationInfo inactiveOperationInfo = inactiveHandleCache.getIfPresent(handle);
    if (inactiveOperationInfo != null) {
      // Operation has been made inactive, so all results should have been fetched already - return empty list.
      LOG.trace("Returning empty result for inactive handle {}", handle);
      return ImmutableList.of();
    }

    Lock nextLock = getOperationInfo(handle).getNextLock();
    nextLock.lock();
    try {
      // Fetch results from Hive
      LOG.trace("Getting results for handle {}", handle);
      OperationHandle opHandle = getOperationHandle(handle);
      List<QueryResult> results = fetchNextResults(opHandle, size);
      QueryStatus status = getStatus(handle);
      if (results.isEmpty() && status.getStatus() == QueryStatus.OpStatus.FINISHED) {
        // Since operation has fetched all the results, handle can be timed out aggressively.
        timeoutAggresively(handle, getResultSchema(handle), status);
      }
      return results;
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    } finally {
      nextLock.unlock();
    }
  }

  protected List<QueryResult> fetchNextResults(OperationHandle operationHandle, int size)
    throws HiveSQLException, ExploreException, HandleNotFoundException {
    startAndWait();

    try {
      if (operationHandle.hasResultSet()) {
        // Rowset is an interface in Hive 13, but a class in Hive 12, so we use reflection
        // so that the compiler does not make assumption on the return type of fetchResults
        Object rowSet = getCliService().fetchResults(operationHandle, FetchOrientation.FETCH_NEXT, size);

        ImmutableList.Builder<QueryResult> rowsBuilder = ImmutableList.builder();
        // if it's the interface
        if (rowSet instanceof Iterable) {
          for (Object[] row : (Iterable<Object[]>) rowSet) {
            List<Object> cols = Lists.newArrayList();
            for (int i = 0; i < row.length; i++) {
              cols.add(row[i]);
            }
            rowsBuilder.add(new QueryResult(cols));
          }
        } else {
          // otherwise do nasty thrift stuff
          Class rowSetClass = Class.forName("org.apache.hive.service.cli.RowSet");
          Method toTRowSetMethod = rowSetClass.getMethod("toTRowSet");
          TRowSet tRowSet = (TRowSet) toTRowSetMethod.invoke(rowSet);
          for (TRow tRow : tRowSet.getRows()) {
            List<Object> cols = Lists.newArrayList();
            for (TColumnValue tColumnValue : tRow.getColVals()) {
              cols.add(tColumnToObject(tColumnValue));
            }
            rowsBuilder.add(new QueryResult(cols));
          }
        }
        return rowsBuilder.build();
      } else {
        return Collections.emptyList();
      }
    } catch (ClassNotFoundException e) {
      throw Throwables.propagate(e);
    } catch (NoSuchMethodException e) {
      throw Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      throw Throwables.propagate(e);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<QueryResult> previewResults(QueryHandle handle)
    throws ExploreException, HandleNotFoundException, SQLException {
    startAndWait();

    if (inactiveHandleCache.getIfPresent(handle) != null) {
      throw new HandleNotFoundException("Query is inactive.", true);
    }

    OperationInfo operationInfo = getOperationInfo(handle);
    Lock previewLock = operationInfo.getPreviewLock();
    previewLock.lock();
    try {
      File previewFile = operationInfo.getPreviewFile();
      if (previewFile != null) {
        try {
          Reader reader = Files.newReader(previewFile, Charsets.UTF_8);
          try {
            return GSON.fromJson(reader, new TypeToken<List<QueryResult>>() { }.getType());
          } finally {
            Closeables.closeQuietly(reader);
          }
        } catch (FileNotFoundException e) {
          LOG.error("Could not retrieve preview result file {}", previewFile, e);
          throw new ExploreException(e);
        }
      }

      try {
        // Create preview results for query
        previewFile = new File(previewsDir, handle.getHandle());
        FileWriter fileWriter = new FileWriter(previewFile);
        try {
          List<QueryResult> results = nextResults(handle, PREVIEW_COUNT);
          GSON.toJson(results, fileWriter);
          operationInfo.setPreviewFile(previewFile);
          return results;
        } finally {
          Closeables.closeQuietly(fileWriter);
        }
      } catch (IOException e) {
        LOG.error("Could not write preview results into file", e);
        throw new ExploreException(e);
      }
    } finally {
      previewLock.unlock();
    }

  }

  @Override
  public List<ColumnDesc> getResultSchema(QueryHandle handle)
    throws ExploreException, HandleNotFoundException, SQLException {
    startAndWait();

    try {
      InactiveOperationInfo inactiveOperationInfo = inactiveHandleCache.getIfPresent(handle);
      if (inactiveOperationInfo != null) {
        // Operation has been made inactive, so return saved schema.
        LOG.trace("Returning saved schema for inactive handle {}", handle);
        return inactiveOperationInfo.getSchema();
      }

      // Fetch schema from hive
      LOG.trace("Getting schema for handle {}", handle);
      OperationHandle operationHandle = getOperationHandle(handle);
      return getResultSchemaInternal(operationHandle);
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    }
  }

  protected List<ColumnDesc> getResultSchemaInternal(OperationHandle operationHandle) throws SQLException {
    ImmutableList.Builder<ColumnDesc> listBuilder = ImmutableList.builder();
    if (operationHandle.hasResultSet()) {
      TableSchema tableSchema = cliService.getResultSetMetadata(operationHandle);
      for (ColumnDescriptor colDesc : tableSchema.getColumnDescriptors()) {
        listBuilder.add(new ColumnDesc(colDesc.getName(), colDesc.getTypeName(),
                                       colDesc.getOrdinalPosition(), colDesc.getComment()));
      }
    }
    return listBuilder.build();
  }

  /**
   * Cancel a running Hive operation. After the operation moves into a {@link QueryStatus.OpStatus#CANCELED},
   * {@link #close(QueryHandle)} needs to be called to release resources.
   *
   * @param handle handle returned by {@link #execute(String)}.
   * @throws ExploreException on any error cancelling operation.
   * @throws HandleNotFoundException when handle is not found.
   * @throws SQLException if there are errors in the SQL statement.
   */
  void cancelInternal(QueryHandle handle) throws ExploreException, HandleNotFoundException, SQLException {
    try {
      InactiveOperationInfo inactiveOperationInfo = inactiveHandleCache.getIfPresent(handle);
      if (inactiveOperationInfo != null) {
        // Operation has been made inactive, so no point in cancelling it.
        LOG.trace("Not running cancel for inactive handle {}", handle);
        return;
      }

      LOG.trace("Cancelling operation {}", handle);
      cliService.cancelOperation(getOperationHandle(handle));
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    }
  }

  @Override
  public void close(QueryHandle handle) throws ExploreException, HandleNotFoundException {
    startAndWait();
    inactiveHandleCache.invalidate(handle);
    activeHandleCache.invalidate(handle);
    DatasetAccessor.closeQuery(handle);
  }

  @Override
  public List<QueryInfo> getQueries() throws ExploreException, SQLException {
    startAndWait();

    List<QueryInfo> result = Lists.newArrayList();
    for (Map.Entry<QueryHandle, OperationInfo> entry : activeHandleCache.asMap().entrySet()) {
      try {
        // we use empty query statement for get tables, get schemas, we don't need to return it this method call.
        if (!entry.getValue().getStatement().isEmpty()) {
          QueryStatus status = getStatus(entry.getKey());
          result.add(new QueryInfo(entry.getValue().getTimestamp(), entry.getValue().getStatement(),
                                   entry.getKey(), status, true));
        }
      } catch (HandleNotFoundException e) {
        // ignore the handle not found exception. this method returns all queries and handle, if the
        // handle is removed from the internal cache, then there is no point returning them from here.
      }
    }

    for (Map.Entry<QueryHandle, InactiveOperationInfo> entry : inactiveHandleCache.asMap().entrySet()) {
      try {
        // we use empty query statement for get tables, get schemas, we don't need to return it this method call.
        if (!entry.getValue().getStatement().isEmpty()) {
          QueryStatus status = getStatus(entry.getKey());
          result.add(new QueryInfo(entry.getValue().getTimestamp(),
                                   entry.getValue().getStatement(), entry.getKey(), status, false));
        }
      } catch (HandleNotFoundException e) {
        // ignore the handle not found exception. this method returns all queries and handle, if the
        // handle is removed from the internal cache, then there is no point returning them from here.
      }
    }
    Collections.sort(result);
    return result;
  }

  void closeInternal(QueryHandle handle, OperationInfo opInfo)
    throws ExploreException, HandleNotFoundException, SQLException {
    try {
      LOG.trace("Closing operation {}", handle);
      cliService.closeOperation(opInfo.getOperationHandle());
    } catch (HiveSQLException e) {
      throw getSqlException(e);
    } finally {
      try {
        closeSession(opInfo.getSessionHandle());
      } finally {
        cleanUp(handle, opInfo);
      }
    }
  }

  private void closeSession(SessionHandle sessionHandle) {
    try {
      cliService.closeSession(sessionHandle);
    } catch (Throwable e) {
      LOG.error("Got error closing session", e);
    }
  }

  /**
   * Starts a long running transaction, and also sets up session configuration.
   * @return configuration for a hive session that contains a transaction, and serialized CDAP configuration and
   * HBase configuration. This will be used by the map-reduce tasks started by Hive.
   * @throws IOException
   */
  protected Map<String, String> startSession() throws IOException {
    Map<String, String> sessionConf = Maps.newHashMap();

    QueryHandle queryHandle = QueryHandle.generate();
    sessionConf.put(Constants.Explore.QUERY_ID, queryHandle.getHandle());
    
    Transaction tx = startTransaction();
    ConfigurationUtil.set(sessionConf, Constants.Explore.TX_QUERY_KEY, TxnCodec.INSTANCE, tx);
    ConfigurationUtil.set(sessionConf, Constants.Explore.CCONF_KEY, CConfCodec.INSTANCE, cConf);
    ConfigurationUtil.set(sessionConf, Constants.Explore.HCONF_KEY, HConfCodec.INSTANCE, hConf);

    return sessionConf;
  }

  /**
   * Returns {@link OperationHandle} associated with Explore {@link QueryHandle}.
   * @param handle explore handle.
   * @return OperationHandle.
   * @throws ExploreException
   */
  protected OperationHandle getOperationHandle(QueryHandle handle) throws ExploreException, HandleNotFoundException {
    return getOperationInfo(handle).getOperationHandle();
  }

  /**
   * Saves information associated with an Hive operation.
   * @param operationHandle {@link OperationHandle} of the Hive operation running.
   * @param sessionHandle {@link SessionHandle} for the Hive operation running.
   * @param sessionConf configuration for the session running the Hive operation.
   * @param statement SQL statement executed with the call.
   * @return {@link QueryHandle} that represents the Hive operation being run.
   */
  protected QueryHandle saveOperationInfo(OperationHandle operationHandle, SessionHandle sessionHandle,
                                     Map<String, String> sessionConf, String statement) {
    QueryHandle handle = QueryHandle.fromId(sessionConf.get(Constants.Explore.QUERY_ID));
    activeHandleCache.put(handle, new OperationInfo(sessionHandle, operationHandle, sessionConf, statement));
    return handle;
  }

  /**
   * Called after a handle has been used to fetch all its results. This handle can be timed out aggressively.
   * It also closes associated transaction.
   *
   * @param handle operation handle.
   */
  private void timeoutAggresively(QueryHandle handle, List<ColumnDesc> schema, QueryStatus status)
    throws HandleNotFoundException {
    OperationInfo opInfo = activeHandleCache.getIfPresent(handle);
    if (opInfo == null) {
      LOG.trace("Could not find OperationInfo for handle {}, it might already have been moved to inactive list",
                handle);
      return;
    }

    closeTransaction(handle, opInfo);

    LOG.trace("Timing out handle {} aggressively", handle);
    inactiveHandleCache.put(handle, new InactiveOperationInfo(opInfo, schema, status));
    activeHandleCache.invalidate(handle);
  }

  private OperationInfo getOperationInfo(QueryHandle handle) throws HandleNotFoundException {
    // First look in running handles and handles that still can be fetched.
    OperationInfo opInfo = activeHandleCache.getIfPresent(handle);
    if (opInfo != null) {
      return opInfo;
    }
    throw new HandleNotFoundException("Invalid handle provided");
  }

  /**
   * Cleans up the metadata associated with active {@link QueryHandle}. It also closes associated transaction.
   * @param handle handle of the running Hive operation.
   */
  protected void cleanUp(QueryHandle handle, OperationInfo opInfo) {
    try {
      if (opInfo.getPreviewFile() != null) {
        opInfo.getPreviewFile().delete();
      }
      closeTransaction(handle, opInfo);
    } finally {
      activeHandleCache.invalidate(handle);
    }
  }

  private Transaction startTransaction() throws IOException {
    Transaction tx = txClient.startLong();
    LOG.trace("Transaction {} started.", tx);
    return tx;
  }

  private void closeTransaction(QueryHandle handle, OperationInfo opInfo) {
    try {
      String txCommitted = opInfo.getSessionConf().get(Constants.Explore.TX_QUERY_CLOSED);
      if (txCommitted != null && Boolean.parseBoolean(txCommitted)) {
        LOG.trace("Transaction for handle {} has already been closed", handle);
        return;
      }

      Transaction tx = ConfigurationUtil.get(opInfo.getSessionConf(),
                                             Constants.Explore.TX_QUERY_KEY,
                                             TxnCodec.INSTANCE);
      LOG.trace("Closing transaction {} for handle {}", tx, handle);

      // Even if changes are empty, we still commit the tx to take care of
      // any side effect changes that SplitReader may have.
      if (!(txClient.commit(tx))) {
        txClient.abort(tx);
        LOG.info("Aborting transaction: {}", tx);
      }
      opInfo.getSessionConf().put(Constants.Explore.TX_QUERY_CLOSED, "true");
    } catch (Throwable e) {
      LOG.error("Got exception while closing transaction.", e);
    }
  }

  private void runCacheCleanup() {
    LOG.trace("Running cache cleanup");
    activeHandleCache.cleanUp();
    inactiveHandleCache.cleanUp();
  }

  // Hive wraps all exceptions, including SQL exceptions in HiveSQLException. We would like to surface the SQL
  // exception to the user, and not other Hive server exceptions. We are using a heuristic to determine whether a
  // HiveSQLException is a SQL exception or not by inspecting the SQLState of HiveSQLException. If SQLState is not
  // null then we surface the SQL exception.
  private RuntimeException getSqlException(HiveSQLException e) throws ExploreException, SQLException {
    if (e.getSQLState() != null) {
      throw e;
    }
    throw new ExploreException(e);
  }

  protected Object tColumnToObject(TColumnValue tColumnValue) throws ExploreException {
    if (tColumnValue.isSetBoolVal()) {
      return tColumnValue.getBoolVal().isValue();
    } else if (tColumnValue.isSetByteVal()) {
      return tColumnValue.getByteVal().getValue();
    } else if (tColumnValue.isSetDoubleVal()) {
      return tColumnValue.getDoubleVal().getValue();
    } else if (tColumnValue.isSetI16Val()) {
      return tColumnValue.getI16Val().getValue();
    } else if (tColumnValue.isSetI32Val()) {
      return tColumnValue.getI32Val().getValue();
    } else if (tColumnValue.isSetI64Val()) {
      return tColumnValue.getI64Val().getValue();
    } else if (tColumnValue.isSetStringVal()) {
      return tColumnValue.getStringVal().getValue();
    }
    throw new ExploreException("Unknown column value encountered: " + tColumnValue);
  }

  /**
  * Helper class to store information about a Hive operation in progress.
  */
  static class OperationInfo {
    private final SessionHandle sessionHandle;
    private final OperationHandle operationHandle;
    private final Map<String, String> sessionConf;
    private final String statement;
    private final long timestamp;
    private final Lock nextLock = new ReentrantLock();
    private final Lock previewLock = new ReentrantLock();

    private File previewFile;

    OperationInfo(SessionHandle sessionHandle, OperationHandle operationHandle,
                  Map<String, String> sessionConf, String statement) {
      this.sessionHandle = sessionHandle;
      this.operationHandle = operationHandle;
      this.sessionConf = sessionConf;
      this.statement = statement;
      this.timestamp = System.currentTimeMillis();
      this.previewFile = null;
    }

    OperationInfo(SessionHandle sessionHandle, OperationHandle operationHandle,
                  Map<String, String> sessionConf, String statement, long timestamp) {
      this.sessionHandle = sessionHandle;
      this.operationHandle = operationHandle;
      this.sessionConf = sessionConf;
      this.statement = statement;
      this.timestamp = timestamp;
    }

    public SessionHandle getSessionHandle() {
      return sessionHandle;
    }

    public OperationHandle getOperationHandle() {
      return operationHandle;
    }

    public Map<String, String> getSessionConf() {
      return sessionConf;
    }

    public String getStatement() {
      return statement;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public File getPreviewFile() {
      return previewFile;
    }

    public void setPreviewFile(File previewFile) {
      this.previewFile = previewFile;
    }

    public Lock getNextLock() {
      return nextLock;
    }

    public Lock getPreviewLock() {
      return previewLock;
    }
  }

  private static class InactiveOperationInfo extends OperationInfo {
    private final List<ColumnDesc> schema;
    private final QueryStatus status;

    private InactiveOperationInfo(OperationInfo operationInfo, List<ColumnDesc> schema, QueryStatus status) {
      super(operationInfo.getSessionHandle(), operationInfo.getOperationHandle(),
            operationInfo.getSessionConf(), operationInfo.getStatement(), operationInfo.getTimestamp());
      this.schema = schema;
      this.status = status;
    }

    public List<ColumnDesc> getSchema() {
      return schema;
    }

    public QueryStatus getStatus() {
      return status;
    }
  }
}
