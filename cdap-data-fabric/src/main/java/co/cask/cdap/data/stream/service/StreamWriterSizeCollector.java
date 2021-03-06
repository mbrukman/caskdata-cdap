/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.service;

import com.google.common.util.concurrent.Service;

/**
 * Keep track of the sizes of the files written by one {@link StreamHandler}.
 */
public interface StreamWriterSizeCollector extends Service {

  // TODO have one implementation of this

  /**
   * Get the total amount of bytes collected for the stream {@code streamName} so far.
   *
   * @param streamName stream name to get the total amount of data collected for
   * @return the total amount of bytes collected for the stream {@code streamName} so far
   */
  long getTotalCollected(String streamName);

  /**
   * Called to notify this manager that {@code dataSize} bytes of data has been ingested by the stream
   * {@code streamName} using the stream handler from which this code is executed. The {@code dataSize}
   * is an incremental size.
   *
   * @param streamName name of the stream that ingested data.
   * @param dataSize amount of data ingested in bytes.
   */
  void received(String streamName, long dataSize);
}
