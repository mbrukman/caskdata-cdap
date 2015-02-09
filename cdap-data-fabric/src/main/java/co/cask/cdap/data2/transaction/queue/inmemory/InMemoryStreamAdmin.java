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

package co.cask.cdap.data2.transaction.queue.inmemory;

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.io.IOException;
import java.util.Map;

/**
 * admin for queues in memory.
 */
@Singleton
public class InMemoryStreamAdmin extends InMemoryQueueAdmin implements StreamAdmin {

  @Inject
  public InMemoryStreamAdmin(InMemoryQueueService queueService) {
    super(queueService);
  }

  @Override
  public void dropAll() throws Exception {
    queueService.resetStreams();
  }

  @Override
  public void configureInstances(Id.Stream streamName, long groupId, int instances) throws Exception {
    configureInstances(QueueName.fromStream(streamName), groupId, instances);
  }

  @Override
  public void configureGroups(Id.Stream streamName, Map<Long, Integer> groupInfo) throws Exception {
    configureGroups(QueueName.fromStream(streamName), groupInfo);
  }

  @Override
  public StreamConfig getConfig(String streamName) {
    // TODO: add support for queue-based stream
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public void updateConfig(StreamConfig config) throws IOException {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public long fetchStreamSize(StreamConfig streamConfig) throws IOException {
    throw new UnsupportedOperationException("Not yet supported");
  }
}
