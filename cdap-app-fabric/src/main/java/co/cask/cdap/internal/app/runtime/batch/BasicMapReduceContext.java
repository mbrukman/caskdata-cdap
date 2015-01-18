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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.logging.context.MapReduceLoggingContext;
import co.cask.tephra.TransactionAware;
import com.google.common.collect.Maps;
import org.apache.hadoop.mapreduce.Job;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Mapreduce job runtime context
 */
public class BasicMapReduceContext extends AbstractContext implements MapReduceContext {

  // TODO: InstanceId is not supported in MR jobs, see CDAP-2
  private final MapReduceSpecification spec;
  private final MapReduceLoggingContext loggingContext;
  private final Map<MetricsScope, MetricsCollector> systemMapperMetrics;
  private final Map<MetricsScope, MetricsCollector> systemReducerMetrics;
  private final long logicalStartTime;
  private final String workflowBatch;
  private final Metrics mapredMetrics;
  private final MetricsCollectionService metricsCollectionService;

  private String inputDatasetName;
  private List<Split> inputDataSelection;

  private String outputDatasetName;
  private Job job;
  private Resources mapperResources;
  private Resources reducerResources;

  public BasicMapReduceContext(Program program,
                               MapReduceMetrics.TaskType type,
                               RunId runId,
                               Arguments runtimeArguments,
                               Set<String> datasets,
                               MapReduceSpecification spec,
                               long logicalStartTime,
                               String workflowBatch,
                               DiscoveryServiceClient discoveryServiceClient,
                               MetricsCollectionService metricsCollectionService,
                               DatasetFramework dsFramework,
                               CConfiguration conf) {
    super(program, runId, runtimeArguments, datasets,
          getMetricCollector(metricsCollectionService, MetricsScope.SYSTEM, program, type, runId.getId()),
          dsFramework, conf, discoveryServiceClient);
    this.logicalStartTime = logicalStartTime;
    this.workflowBatch = workflowBatch;
    this.metricsCollectionService = metricsCollectionService;

    if (metricsCollectionService != null) {
      this.systemMapperMetrics = Maps.newHashMap();
      this.systemReducerMetrics = Maps.newHashMap();
      Map<MetricsScope, MetricsCollector> systemMetrics = Maps.newHashMap();
      for (MetricsScope scope : MetricsScope.values()) {
        this.systemMapperMetrics.put(scope, getMetricCollector(metricsCollectionService, scope, program,
                                                               MapReduceMetrics.TaskType.Mapper, runId.getId()));
        this.systemReducerMetrics.put(scope, getMetricCollector(metricsCollectionService, scope, program,
                                                                MapReduceMetrics.TaskType.Reducer, runId.getId()));
        systemMetrics.put(scope, getMetricCollector(metricsCollectionService, scope, program, null, runId.getId()));
      }
      // for user metrics.  type can be null if its not in a map or reduce task, but in the yarn container that
      // launches the mapred job.
      this.mapredMetrics = (type == null) ?
        null : new ProgramUserMetrics(getMetricCollector(metricsCollectionService, MetricsScope.USER,
                                                         program, type, runId.getId()));
    } else {
      this.systemMapperMetrics = null;
      this.systemReducerMetrics = null;
      this.mapredMetrics = null;
    }
    this.loggingContext = new MapReduceLoggingContext(getNamespaceId(), getApplicationId(), getProgramName());
    this.spec = spec;
    this.mapperResources = spec.getMapperResources();
    this.reducerResources = spec.getReducerResources();
  }

  @Override
  public String toString() {
    return String.format("job=%s,=%s",
                         spec.getName(), super.toString());
  }

  @Override
  public MapReduceSpecification getSpecification() {
    return spec;
  }

  @Override
  public long getLogicalStartTime() {
    return logicalStartTime;
  }

  /**
   * Returns the name of the Batch job when running inside workflow. Otherwise, return null.
   */
  public String getWorkflowBatch() {
    return workflowBatch;
  }

  public void setJob(Job job) {
    this.job = job;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getHadoopJob() {
    return (T) job;
  }

  @Override
  public void setInput(String datasetName, List<Split> splits) {
    this.inputDatasetName = datasetName;
    this.inputDataSelection = splits;
  }

  @Override
  public void setOutput(String datasetName) {
    this.outputDatasetName = datasetName;
  }

  @Override
  public void setMapperResources(Resources resources) {
    this.mapperResources = resources;
  }

  @Override
  public void setReducerResources(Resources resources) {
    this.reducerResources = resources;
  }

  private static MetricsCollector getMetricCollector(MetricsCollectionService service, MetricsScope scope,
                                                     Program program, MapReduceMetrics.TaskType type, String runId) {
    if (service == null) {
      return null;
    }
    Map<String, String> tags = Maps.newHashMap(getMetricsContext(program, runId));
    if (type != null) {
      tags.put(Constants.Metrics.Tag.MR_TASK_TYPE, type.getId());
    }
    return service.getCollector(scope, tags);
  }

  @Override
  public Metrics getMetrics() {
    return mapredMetrics;
  }

  public MetricsCollectionService getMetricsCollectionService() {
    return metricsCollectionService;
  }

  public MetricsCollector getSystemMapperMetrics() {
    return systemMapperMetrics.get(MetricsScope.SYSTEM);
  }

  public MetricsCollector getSystemReducerMetrics() {
    return systemReducerMetrics.get(MetricsScope.SYSTEM);
  }

  public MetricsCollector getSystemMapperMetrics(MetricsScope scope) {
    return systemMapperMetrics.get(scope);
  }

  public MetricsCollector getSystemReducerMetrics(MetricsScope scope) {
    return systemReducerMetrics.get(scope);
  }

  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  @Nullable
  public String getInputDatasetName() {
    return inputDatasetName;
  }

  public List<Split> getInputDataSelection() {
    return inputDataSelection;
  }

  @Nullable
  public String getOutputDatasetName() {
    return outputDatasetName;
  }

  public Resources getMapperResources() {
    return mapperResources;
  }

  public Resources getReducerResources() {
    return reducerResources;
  }

  public void flushOperations() throws Exception {
    for (TransactionAware txAware : getDatasetInstantiator().getTransactionAware()) {
      txAware.commitTx();
    }
  }
}
