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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.lang.Reflections;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Wraps user-defined implementation of {@link Mapper} class which allows perform extra configuration.
 */
public class MapperWrapper extends Mapper {

  private static final String ATTR_MAPPER_CLASS = "c.mapper.class";

  private static final Logger LOG = LoggerFactory.getLogger(MapperWrapper.class);

  /**
   * Wraps the mapper defined in the job with this {@link MapperWrapper} if it is defined.
   * @param job
   */
  public static void wrap(Job job) {
    // NOTE: we don't use job.getMapperClass() as we don't need to load user class here
    Configuration conf = job.getConfiguration();
    String mapClass = conf.get(MRJobConfig.MAP_CLASS_ATTR);
    if (mapClass != null) {
      conf.set(MapperWrapper.ATTR_MAPPER_CLASS, mapClass);
      job.setMapperClass(MapperWrapper.class);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    MapReduceContextProvider mrContextProvider =
      new MapReduceContextProvider(context, MapReduceMetrics.TaskType.Mapper);
    final BasicMapReduceContext basicMapReduceContext = mrContextProvider.get();
    context.getConfiguration().setClassLoader(basicMapReduceContext.getProgram().getClassLoader());
    basicMapReduceContext.getMetricsCollectionService().startAndWait();

    // now that the context is created, we need to make sure to properly close all datasets of the context
    try {
      String userMapper = context.getConfiguration().get(ATTR_MAPPER_CLASS);
      Mapper delegate = createMapperInstance(basicMapReduceContext.getProgram().getClassLoader(), userMapper);

      // injecting runtime components, like datasets, etc.
      try {
        Reflections.visit(delegate, TypeToken.of(delegate.getClass()),
                          new PropertyFieldSetter(basicMapReduceContext.getSpecification().getProperties()),
                          new MetricsFieldSetter(basicMapReduceContext.getMetrics()),
                          new DataSetFieldSetter(basicMapReduceContext));
      } catch (Throwable t) {
        LOG.error("Failed to inject fields to {}.", delegate.getClass(), t);
        throw Throwables.propagate(t);
      }

      LoggingContextAccessor.setLoggingContext(basicMapReduceContext.getLoggingContext());

      // this is a hook for periodic flushing of changes buffered by datasets (to avoid OOME)
      WrappedMapper.Context flushingContext = createAutoFlushingContext(context, basicMapReduceContext);

      ClassLoader oldClassLoader;
      if (delegate instanceof ProgramLifecycle) {
        oldClassLoader = ClassLoaders.setContextClassLoader(basicMapReduceContext.getProgram().getClassLoader());
        try {
          ((ProgramLifecycle<BasicMapReduceContext>) delegate).initialize(basicMapReduceContext);
        } catch (Exception e) {
          LOG.error("Failed to initialize mapper with {}", basicMapReduceContext, e);
          throw Throwables.propagate(e);
        } finally {
          ClassLoaders.setContextClassLoader(oldClassLoader);
        }
      }

      oldClassLoader = ClassLoaders.setContextClassLoader(basicMapReduceContext.getProgram().getClassLoader());
      try {
        delegate.run(flushingContext);
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
      }

      // sleep to allow metrics to be written
      TimeUnit.SECONDS.sleep(2L);

      // transaction is not finished, but we want all operations to be dispatched (some could be buffered in
      // memory by tx agent
      try {
        basicMapReduceContext.flushOperations();
      } catch (Exception e) {
        LOG.error("Failed to flush operations at the end of mapper of {}", basicMapReduceContext, e);
        throw Throwables.propagate(e);
      }

      if (delegate instanceof ProgramLifecycle) {
        oldClassLoader = ClassLoaders.setContextClassLoader(basicMapReduceContext.getProgram().getClassLoader());
        try {
          ((ProgramLifecycle<? extends RuntimeContext>) delegate).destroy();
        } catch (Exception e) {
          LOG.error("Error during destroy of mapper {}", basicMapReduceContext, e);
          // Do nothing, try to finish
        } finally {
          ClassLoaders.setContextClassLoader(oldClassLoader);
        }
      }

    } finally {
      basicMapReduceContext.close();
      basicMapReduceContext.getMetricsCollectionService().stop();
    }
  }

  private WrappedMapper.Context createAutoFlushingContext(final Context context,
                                                          final BasicMapReduceContext basicMapReduceContext) {
    // NOTE: we will change auto-flush to take into account size of buffered data, so no need to do/test a lot with
    //       current approach
    final int flushFreq = context.getConfiguration().getInt("c.mapper.flush.freq", 10000);

    @SuppressWarnings("unchecked")
    WrappedMapper.Context flushingContext = new WrappedMapper().new Context(context) {
      private int processedRecords = 0;

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean result = super.nextKeyValue();
        if (++processedRecords > flushFreq) {
          try {
            LOG.info("Flushing dataset operations...");
            basicMapReduceContext.flushOperations();
          } catch (Exception e) {
            LOG.error("Failed to persist changes", e);
            throw Throwables.propagate(e);
          }
          processedRecords = 0;
        }
        return result;
      }
    };
    return flushingContext;
  }

  private Mapper createMapperInstance(ClassLoader classLoader, String userMapper) {
    try {
      return (Mapper) classLoader.loadClass(userMapper).newInstance();
    } catch (Exception e) {
      LOG.error("Failed to create instance of the user-defined Mapper class: " + userMapper);
      throw Throwables.propagate(e);
    }
  }
}
