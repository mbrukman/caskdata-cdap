/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.metrics.store.timeseries;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Represents measure in time with tags assigned to it
 */
public final class Fact {
  private final List<TagValue> tagValues;
  private final MeasureType measureType;
  private final String measureName;
  private final TimeValue timeValue;

  public Fact(List<TagValue> tagValues, MeasureType measureType, String measureName, TimeValue timeValue) {
    this.tagValues = ImmutableList.copyOf(tagValues);
    this.measureType = measureType;
    this.measureName = measureName;
    this.timeValue = timeValue;
  }

  public List<TagValue> getTagValues() {
    return tagValues;
  }

  public MeasureType getMeasureType() {
    return measureType;
  }

  public String getMeasureName() {
    return measureName;
  }

  public TimeValue getTimeValue() {
    return timeValue;
  }
}
