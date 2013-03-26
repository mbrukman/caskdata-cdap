package com.continuuity.internal.app.runtime.batch.hadoop;

import com.continuuity.api.batch.hadoop.MapReduce;
import com.continuuity.api.batch.hadoop.MapReduceSpecification;
import com.continuuity.base.Cancellable;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.runtime.batch.BasicBatchContext;
import com.google.common.util.concurrent.Service;

/**
 * Performs the actual execution of mapreduce job.
 */
public interface MapReduceRuntimeService extends Service {
  /**
   * Submits the mapreduce job for execution.
   * @param job job to run
   * @param spec job spec
   * @param jobJarLocation location of the job jar
   * @param context runtime context
   * @throws Exception
   */
  Cancellable submit(MapReduce job, MapReduceSpecification spec,
              Location jobJarLocation, BasicBatchContext context, JobFinishCallback callback) throws Exception;

  public static interface JobFinishCallback {
    void onFinished(boolean success);
  }
}
