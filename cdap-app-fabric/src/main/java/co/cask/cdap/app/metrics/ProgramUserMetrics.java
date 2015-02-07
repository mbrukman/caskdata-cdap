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

package co.cask.cdap.app.metrics;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Implementation of {@link Metrics} for user-defined metrics.
 * Metrics will be emitted through {@link MetricsCollectionService}.
 */
// todo: was made Externalizable for Spark. Seems wrong that we try to pass it
public class ProgramUserMetrics implements Metrics, Externalizable {
  private static final long serialVersionUID = -5913108632034346101L;

  private final MetricsCollector metricsCollector;

  /** For serde purposes only */
  public ProgramUserMetrics() {
    metricsCollector = null;
  }

  public ProgramUserMetrics(MetricsCollector metricsCollector) {
    this.metricsCollector = metricsCollector.childCollector(Constants.Metrics.Tag.SCOPE, "user");
  }

  @Override
  public void count(String metricName, int delta) {
    metricsCollector.increment(metricName, delta);
  }

  @Override
  public void gauge(String metricName, long value) {
    metricsCollector.gauge(metricName, value);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // do nothing
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // do nothing
  }
}
