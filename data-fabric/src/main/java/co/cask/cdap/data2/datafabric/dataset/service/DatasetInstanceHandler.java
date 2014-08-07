/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.HandlerException;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpResponse;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.explore.client.DatasetExploreFacade;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handles dataset instance management calls.
 */
// todo: do we want to make it authenticated? or do we treat it always as "internal" piece?
@Path(Constants.Gateway.GATEWAY_VERSION)
public class DatasetInstanceHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceHandler.class);
  private static final Gson GSON = new Gson();

  private final DatasetTypeManager implManager;
  private final DatasetInstanceManager instanceManager;
  private final DatasetOpExecutor opExecutorClient;
  private final DatasetExploreFacade datasetExploreFacade;

  private final CConfiguration conf;

  @Inject
  public DatasetInstanceHandler(DatasetTypeManager implManager, DatasetInstanceManager instanceManager,
                                DatasetOpExecutor opExecutorClient, DatasetExploreFacade datasetExploreFacade,
                                CConfiguration conf) {
    this.opExecutorClient = opExecutorClient;
    this.implManager = implManager;
    this.instanceManager = instanceManager;
    this.datasetExploreFacade = datasetExploreFacade;
    this.conf = conf;
  }

  @GET
  @Path("/data/datasets/")
  public void list(HttpRequest request, final HttpResponder responder) {
    Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();

    // if meta is true, then DatasetMeta objects will be returned by this endpoint
    // Otherwise, by default and for any other value, DatasetSpecification objects will be returned.
    boolean isMeta = queryParams.containsKey("meta") && queryParams.get("meta").contains("true");

    // If explorable is true, only explorable datasets (defined as ones for which a Hive table exists) will
    // be returned. If it is false, only non-explorable datasets will be returned.
    // If this option is not set, or neither true nor false, then all datasets are returned.
    boolean explorableDatasetsOption = queryParams.containsKey("explorable")
      && (queryParams.get("explorable").contains("true") || queryParams.get("explorable").contains("false"));
    boolean getExplorableDatasets = explorableDatasetsOption && queryParams.get("explorable").contains("true");

    Collection<DatasetSpecification> datasetSpecifications = instanceManager.getAll();

    if (explorableDatasetsOption) {
      try {
        // Do a join/disjoin of the list of datasets, and the list of Hive tables
        List<String> hiveTables = datasetExploreFacade.getExplorableDatasetsTableNames();
        ImmutableList.Builder<?> joinBuilder = ImmutableList.builder();

        for (DatasetSpecification spec : datasetSpecifications) {
          // True if this dataset has a Hive table associated with it
          boolean isExplorable = hiveTables.contains(DatasetExploreFacade.getHiveTableName(spec.getName()));
          if (isExplorable && getExplorableDatasets || !isExplorable && !getExplorableDatasets) {
            if (isMeta) {
              // Return DatasetMeta objects
              DatasetMeta meta;
              if (isExplorable) {
                // Add dataset Hive table name to the DatasetMeta object
                meta = new DatasetMeta(spec, implManager.getTypeInfo(spec.getType()),
                                       DatasetExploreFacade.getHiveTableName(spec.getName()));
              } else {
                meta = new DatasetMeta(spec, implManager.getTypeInfo(spec.getType()), null);
              }
              joinBuilder.add(meta);
            } else {
              // Return DatasetSpecification objects
              joinBuilder.add(spec);
            }
          }
        }
        responder.sendJson(HttpResponseStatus.OK, joinBuilder.build());
        return;
      } catch (Throwable t) {
        LOG.error("Caught exception while listing explorable datasets", t);
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        return;
      }
    }
    if (isMeta) {
      ImmutableList.Builder<DatasetMeta> builder = ImmutableList.builder();
      for (DatasetSpecification spec : datasetSpecifications) {
        builder.add(new DatasetMeta(spec, implManager.getTypeInfo(spec.getType()), null));
      }
      responder.sendJson(HttpResponseStatus.OK, builder.build());
    } else {
      responder.sendJson(HttpResponseStatus.OK, datasetSpecifications);
    }
  }

  @DELETE
  @Path("/data/unrecoverable/datasets/")
  public void deleteAll(HttpRequest request, final HttpResponder responder) throws Exception {
    if (!conf.getBoolean(Constants.Dangerous.UNRECOVERABLE_RESET,
                         Constants.Dangerous.DEFAULT_UNRECOVERABLE_RESET)) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
      return;
    }

    boolean succeeded = true;
    for (DatasetSpecification spec : instanceManager.getAll()) {
      try {
        // It is okay if dataset not exists: someone may be deleting it at same time
        dropDataset(spec);
      } catch (Exception e) {
        String msg = String.format("Cannot delete dataset %s: executing delete() failed, reason: %s",
                                   spec.getName(), e.getMessage());
        LOG.warn(msg, e);
        succeeded = false;
        // we continue deleting if something wring happens.
        // todo: Will later be improved by doing all in async: see REACTOR-200
      }
    }

    responder.sendStatus(succeeded ? HttpResponseStatus.OK : HttpResponseStatus.INTERNAL_SERVER_ERROR);
  }

  @GET
  @Path("/data/datasets/{name}")
  public void getInfo(HttpRequest request, final HttpResponder responder,
                      @PathParam("name") String name) {
    DatasetSpecification spec = instanceManager.get(name);
    if (spec == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      DatasetMeta info = new DatasetMeta(spec, implManager.getTypeInfo(spec.getType()), null);
      responder.sendJson(HttpResponseStatus.OK, info);
    }
  }

  /**
   * Creates a new Dataset or updates existing Dataset specification's
   * properties if an optional update parameter in the body is set to true, {@link DatasetInstanceConfiguration}
   * is constructed based on request and appropriate action is performed
   */
  @PUT
  @Path("/data/datasets/{name}")
  public void createOrUpdate(HttpRequest request, final HttpResponder responder,
                  @PathParam("name") String name) {
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()));

    DatasetInstanceConfiguration creationProperties = GSON.fromJson(reader, DatasetInstanceConfiguration.class);

    LOG.info("Creating dataset {}, type name: {}, typeAndProps: {}",
             name, creationProperties.getTypeName(), creationProperties.getProperties());

    DatasetSpecification existing = instanceManager.get(name);
    if (existing != null) {
      String message = String.format("Cannot create dataset %s: instance with same name already exists %s",
                                     name, existing);
      LOG.warn(message);
      responder.sendError(HttpResponseStatus.CONFLICT, message);
      return;
    }

    createDatasetInstance(creationProperties, name, responder, "create");

    // Enable ad-hoc exploration of dataset
    // Note: today explore enable is not transactional with dataset create - REACTOR-314
    try {
      datasetExploreFacade.enableExplore(name);
    } catch (Exception e) {
      String msg = String.format("Cannot enable exploration of dataset instance %s of type %s: %s",
                                 name, creationProperties.getProperties(), e.getMessage());
      LOG.error(msg, e);
      // TODO: at this time we want to still allow using dataset even if it cannot be used for exploration
      //responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, msg);
      //return;
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Updates an existing Dataset specification properties  {@link DatasetInstanceConfiguration}
   * is constructed based on request and the Dataset instance is updated.
   */
  @PUT
  @Path("/data/datasets/{name}/properties")
  public void update(HttpRequest request, final HttpResponder responder,
                     @PathParam("name") String name) {
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()));

    DatasetInstanceConfiguration creationProperties = GSON.fromJson(reader, DatasetInstanceConfiguration.class);

    LOG.info("Update dataset {}, type name: {}, typeAndProps: {}",
             name, creationProperties.getTypeName(), creationProperties.getProperties());
    DatasetSpecification existing = instanceManager.get(name);

    if (existing == null) {
      // update is true , but dataset instance does not exist, return 404.
      responder.sendError(HttpResponseStatus.NOT_FOUND,
                          String.format("Dataset Instance %s does not exist to update", name));
      return;
    }

    if (!existing.getType().equals(creationProperties.getTypeName())) {
      String  message = String.format("Cannot update dataset %s instance with a different type, existing type is %s",
                                      name, existing.getType());
      LOG.warn(message);
      responder.sendError(HttpResponseStatus.CONFLICT, message);
      return;
    }
    createDatasetInstance(creationProperties, name, responder, "update");
    // Enable ad-hoc exploration of dataset
    // Note: today explore enable is not transactional with dataset create - REACTOR-314

    try {
      datasetExploreFacade.disableExplore(name);
      datasetExploreFacade.enableExplore(name);
    } catch (Exception e) {
      String msg = String.format("Cannot enable exploration of dataset instance %s of type %s: %s",
                                 name, creationProperties.getProperties(), e.getMessage());
      LOG.error(msg, e);
      // TODO: at this time we want to still allow using dataset even if it cannot be used for exploration
      //responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, msg);
      //return;
    }
    //caling admin upgrade, after updating specification
    executeAdmin(request, responder, name, "upgrade");
  }

  private void createDatasetInstance(DatasetInstanceConfiguration creationProperties,
                                     String name, HttpResponder responder, String operation) {
    DatasetTypeMeta typeMeta = implManager.getTypeInfo(creationProperties.getTypeName());
    if (typeMeta == null) {
      String message = String.format("Cannot %s dataset %s: unknown type %s",
                                     operation, name, creationProperties.getTypeName());
      LOG.warn(message);
      responder.sendError(HttpResponseStatus.NOT_FOUND, message);
      return;
    }

    // Note how we execute configure() via opExecutorClient (outside of ds service) to isolate running user code
    DatasetSpecification spec;
    try {
      spec = opExecutorClient.create(name, typeMeta,
                                     DatasetProperties.builder().addAll(creationProperties.getProperties()).build());
    } catch (Exception e) {
      String msg = String.format("Cannot %s dataset %s of type %s: executing create() failed, reason: %s",
                                 operation, name, creationProperties.getTypeName(), e.getMessage());
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }
    instanceManager.add(spec);
  }

  @DELETE
  @Path("/data/datasets/{name}")
  public void drop(HttpRequest request, final HttpResponder responder,
                   @PathParam("name") String name) {
    LOG.info("Deleting dataset {}", name);

    DatasetSpecification spec = instanceManager.get(name);
    if (spec == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    try {
      if (!dropDataset(spec)) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
    } catch (Exception e) {
      String msg = String.format("Cannot delete dataset %s: executing delete() failed, reason: %s",
                                 name, e.getMessage());
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/data/datasets/{name}/admin/{method}")
  public void executeAdmin(HttpRequest request, final HttpResponder responder,
                           @PathParam("name") String instanceName,
                           @PathParam("method") String method) {

    try {
      Object result = null;
      String message = null;

      // NOTE: one cannot directly call create and drop, instead this should be called thru
      //       POST/DELETE @ /data/datasets/{instance-id}. Because we must create/drop metadata for these at same time
      if (method.equals("exists")) {
        result = opExecutorClient.exists(instanceName);
      } else if (method.equals("truncate")) {
        opExecutorClient.truncate(instanceName);
      } else if (method.equals("upgrade")) {
        opExecutorClient.upgrade(instanceName);
      } else {
        throw new HandlerException(HttpResponseStatus.NOT_FOUND, "Invalid admin operation: " + method);
      }

      DatasetAdminOpResponse response = new DatasetAdminOpResponse(result, message);
      responder.sendJson(HttpResponseStatus.OK, response);
    } catch (HandlerException e) {
      LOG.debug("Handler error", e);
      responder.sendStatus(e.getFailureStatus());
    } catch (Exception e) {
      LOG.error("Error executing admin operation {} for dataset instance {}", method, instanceName, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/data/datasets/{name}/data/{method}")
  public void executeDataOp(HttpRequest request, final HttpResponder responder,
                            @PathParam("name") String instanceName,
                            @PathParam("method") String method) {
    // todo: execute data operation
    responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }

  /**
   * Drops a dataset.
   * @param spec specification of dataset to be dropped.
   * @return true if dropped successfully, false if dataset is not found.
   * @throws Exception on error.
   */
  private boolean dropDataset(DatasetSpecification spec) throws Exception {
    String name = spec.getName();

    // First disable ad-hoc exploration of dataset
    // Note: today explore disable is not transactional with dataset delete - REACTOR-314
    try {
      datasetExploreFacade.disableExplore(name);
    } catch (ExploreException e) {
      String msg = String.format("Cannot disable exploration of dataset instance %s: %s",
                                 name, e.getMessage());
      LOG.error(msg, e);
      // TODO: at this time we want to still drop dataset even if it cannot be disabled for exploration
//      throw e;
    }

    if (!instanceManager.delete(name)) {
      return false;
    }

    opExecutorClient.drop(spec, implManager.getTypeInfo(spec.getType()));
    return true;
  }
}