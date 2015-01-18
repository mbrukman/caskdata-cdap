/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.adapter.AdapterSpecification;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Store for application metadata
 */
public class AppMetadataStore extends MetadataStoreDataset {
  private static final Logger LOG = LoggerFactory.getLogger(AppMetadataStore.class);

  private static final Gson GSON;

  static {
    GsonBuilder builder = new GsonBuilder();
    ApplicationSpecificationAdapter.addTypeAdapters(builder);
    GSON = builder.create();
  }

  private static final String TYPE_APP_META = "appMeta";
  private static final String TYPE_STREAM = "stream";
  private static final String TYPE_RUN_RECORD_STARTED = "runRecordStarted";
  private static final String TYPE_RUN_RECORD_COMPLETED = "runRecordCompleted";
  private static final String TYPE_PROGRAM_ARGS = "programArgs";
  private static final String TYPE_NAMESPACE = "namespace";
  private static final String TYPE_ADAPTER = "adapter";

  public AppMetadataStore(Table table) {
    super(table);
  }

  @Override
  protected <T> byte[] serialize(T value) {
    return Bytes.toBytes(GSON.toJson(value));
  }

  @Override
  protected <T> T deserialize(byte[] serialized, Class<T> classOfT) {
    return GSON.fromJson(Bytes.toString(serialized), classOfT);
  }

  @Nullable
  public ApplicationMeta getApplication(String namespaceId, String appId) {
    return get(new Key.Builder().add(TYPE_APP_META, namespaceId, appId).build(), ApplicationMeta.class);
  }

  public List<ApplicationMeta> getAllApplications(String namespaceId) {
    return list(new Key.Builder().add(TYPE_APP_META, namespaceId).build(), ApplicationMeta.class);
  }

  public void writeApplication(String namespaceId, String appId, ApplicationSpecification spec,
                               String archiveLocation) {
    // NOTE: we use Gson underneath to do serde, as it doesn't serialize inner classes (which we use everywhere for
    //       specs - see forwarding specs), we want to wrap spec with DefaultApplicationSpecification
    spec = DefaultApplicationSpecification.from(spec);
    write(new Key.Builder().add(TYPE_APP_META, namespaceId, appId).build(),
          new ApplicationMeta(appId, spec, archiveLocation));
  }

  public void deleteApplication(String namespaceId, String appId) {
    deleteAll(new Key.Builder().add(TYPE_APP_META, namespaceId, appId).build());
  }

  public void deleteApplications(String namespaceId) {
    deleteAll(new Key.Builder().add(TYPE_APP_META, namespaceId).build());
  }

  // todo: do we need appId? may be use from appSpec?
  public void updateAppSpec(String namespaceId, String appId, ApplicationSpecification spec) {
    // NOTE: we use Gson underneath to do serde, as it doesn't serialize inner classes (which we use everywhere for
    //       specs - see forwarding specs), we want to wrap spec with DefaultApplicationSpecification
    spec = DefaultApplicationSpecification.from(spec);
    LOG.trace("App spec to be updated: id: {}: spec: {}", appId, GSON.toJson(spec));
    Key key = new Key.Builder().add(TYPE_APP_META, namespaceId, appId).build();
    ApplicationMeta existing = get(key, ApplicationMeta.class);
    if (existing == null) {
      String msg = String.format("No meta for namespace %s app %s exists", namespaceId, appId);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    LOG.trace("Application exists in mds: id: {}, spec: {}", existing);
    ApplicationMeta updated = ApplicationMeta.updateSpec(existing, spec);
    write(key, updated);

    for (StreamSpecification stream : spec.getStreams().values()) {
      writeStream(namespaceId, stream);
    }
  }

  public void recordProgramStart(String namespaceId, String appId, String programId, String pid, long startTs) {
      write(new Key.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId, appId, programId, pid).build(),
            new RunRecord(pid, startTs, null, ProgramRunStatus.RUNNING));
  }

  public void recordProgramStop(String namespaceId, String appId, String programId,
                                String pid, long stopTs, ProgramController.State endStatus) {
    Key key = new Key.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId, appId, programId, pid).build();
    RunRecord started = get(key, RunRecord.class);
    if (started == null) {
      String msg = String.format("No meta for started run record for namespace %s app %s program %s pid %s exists",
                                 namespaceId, appId, programId, pid);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    deleteAll(key);

    key = new Key.Builder()
      .add(TYPE_RUN_RECORD_COMPLETED, namespaceId, appId, programId)
      .add(getInvertedTsKeyPart(started.getStartTs()))
      .add(pid).build();
    write(key, new RunRecord(started, stopTs, endStatus.getRunStatus()));
  }

  public List<RunRecord> getRuns(String namespaceId, String appId, String programId,
                                 ProgramRunStatus status,
                                 long startTime, long endTime, int limit) {
    if (status.equals(ProgramRunStatus.ALL)) {
      List<RunRecord> resultRecords = Lists.newArrayList();
      resultRecords.addAll(getActiveRuns(namespaceId, appId, programId, startTime, endTime, limit));
      resultRecords.addAll(getHistoricalRuns(namespaceId, appId, programId, status, startTime, endTime, limit));
      return resultRecords;
    } else if (status.equals(ProgramRunStatus.RUNNING)) {
      return getActiveRuns(namespaceId, appId, programId, startTime, endTime, limit);
    } else {
      return getHistoricalRuns(namespaceId, appId, programId, status, startTime, endTime, limit);
    }
  }

  private List<RunRecord> getActiveRuns(String namespaceId, String appId, String programId,
                                        final long startTime, final long endTime, int limit) {
    Key activeKey = new Key.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId, appId, programId).build();
    Key start = new Key.Builder(activeKey).add(getInvertedTsKeyPart(endTime)).build();
    Key stop = new Key.Builder(activeKey).add(getInvertedTsKeyPart(startTime)).build();
    return list(start, stop, RunRecord.class, limit, Predicates.<RunRecord>alwaysTrue());
  }

  private List<RunRecord> getHistoricalRuns(String namespaceId, String appId, String programId,
                                            ProgramRunStatus status,
                                            final long startTime, final long endTime, int limit) {
    Key historyKey = new Key.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId, appId, programId).build();
    Key start = new Key.Builder(historyKey).add(getInvertedTsKeyPart(endTime)).build();
    Key stop = new Key.Builder(historyKey).add(getInvertedTsKeyPart(startTime)).build();
    if (status.equals(ProgramRunStatus.ALL)) {
      //return all records (successful and failed)
      return list(start, stop, RunRecord.class, limit, Predicates.<RunRecord>alwaysTrue());
    }
    if (status.equals(ProgramRunStatus.COMPLETED)) {
      return list(start, stop, RunRecord.class, limit, getPredicate(ProgramController.State.STOPPED));
    }
    return list(start, stop, RunRecord.class, limit, getPredicate(ProgramController.State.ERROR));
  }

  private Predicate<RunRecord> getPredicate(final ProgramController.State state) {
    return new Predicate<RunRecord>() {
      @Override
      public boolean apply(RunRecord record) {
        return record.getStatus().equals(state.getRunStatus());
      }
    };
  }

  private long getInvertedTsKeyPart(long endTime) {
    return Long.MAX_VALUE - endTime;
  }

  public void writeStream(String namespaceId, StreamSpecification spec) {
    write(new Key.Builder().add(TYPE_STREAM, namespaceId, spec.getName()).build(), spec);
  }

  public StreamSpecification getStream(String namespaceId, String name) {
    return get(new Key.Builder().add(TYPE_STREAM, namespaceId, name).build(), StreamSpecification.class);
  }

  public List<StreamSpecification> getAllStreams(String namespaceId) {
    return list(new Key.Builder().add(TYPE_STREAM, namespaceId).build(), StreamSpecification.class);
  }

  public void deleteAllStreams(String namespaceId) {
    deleteAll(new Key.Builder().add(TYPE_STREAM, namespaceId).build());
  }

  public void deleteStream(String namespaceId, String name) {
    deleteAll(new Key.Builder().add(TYPE_STREAM, namespaceId, name).build());
  }

  public void writeProgramArgs(String namespaceId, String appId, String programName, Map<String, String> args) {
    write(new Key.Builder().add(TYPE_PROGRAM_ARGS, namespaceId, appId, programName).build(), new ProgramArgs(args));
  }

  public ProgramArgs getProgramArgs(String namespaceId, String appId, String programName) {
    return get(new Key.Builder().add(TYPE_PROGRAM_ARGS, namespaceId, appId, programName).build(), ProgramArgs.class);
  }

  public void deleteProgramArgs(String namespaceId, String appId, String programName) {
    deleteAll(new Key.Builder().add(TYPE_PROGRAM_ARGS, namespaceId, appId, programName).build());
  }

  public void deleteProgramArgs(String namespaceId, String appId) {
    deleteAll(new Key.Builder().add(TYPE_PROGRAM_ARGS, namespaceId, appId).build());
  }

  public void deleteProgramArgs(String namespaceId) {
    deleteAll(new Key.Builder().add(TYPE_PROGRAM_ARGS, namespaceId).build());
  }

  public void deleteProgramHistory(String namespaceId, String appId) {
    deleteAll(new Key.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId, appId).build());
    deleteAll(new Key.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId, appId).build());
  }

  public void deleteProgramHistory(String namespaceId) {
    deleteAll(new Key.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId).build());
    deleteAll(new Key.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId).build());
  }

  public void createNamespace(NamespaceMeta metadata) {
    write(getNamespaceKey(metadata.getId()), metadata);
  }

  public NamespaceMeta getNamespace(Id.Namespace id) {
    return get(getNamespaceKey(id.getId()), NamespaceMeta.class);
  }

  public void deleteNamespace(Id.Namespace id) {
    deleteAll(getNamespaceKey(id.getId()));
  }

  public List<NamespaceMeta> listNamespaces() {
    return list(getNamespaceKey(null), NamespaceMeta.class);
  }

  public void writeAdapter(Id.Namespace id, AdapterSpecification spec) {
    write(new Key.Builder().add(TYPE_ADAPTER, id.getId(), spec.getName()).build(), spec);
  }

  public AdapterSpecification getAdapter(Id.Namespace id, String name) {
    return get(new Key.Builder().add(TYPE_ADAPTER, id.getId(), name).build(), AdapterSpecification.class);
  }

  public List<AdapterSpecification> getAllAdapters(Id.Namespace id) {
    return list(new Key.Builder().add(TYPE_ADAPTER, id.getId()).build(), AdapterSpecification.class);
  }

  public void deleteAdapter(Id.Namespace id, String name) {
    deleteAll(new Key.Builder().add(TYPE_ADAPTER, id.getId(), name).build());
  }

  public void deleteAllAdapters(Id.Namespace id) {
    deleteAll(new Key.Builder().add(TYPE_ADAPTER, id.getId()).build());
  }

  private Key getNamespaceKey(@Nullable String name) {
    Key.Builder builder = new MetadataStoreDataset.Key.Builder().add(TYPE_NAMESPACE);
    if (null != name) {
      builder.add(name);
    }
    return builder.build();
  }
}
