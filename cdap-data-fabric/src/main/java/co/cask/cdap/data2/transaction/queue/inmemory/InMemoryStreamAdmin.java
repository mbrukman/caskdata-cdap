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

import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.io.IOException;

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
  public StreamConfig getConfig(String streamName) {
    // TODO: add support for queue-based stream
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public void updateConfig(StreamConfig config) throws IOException {
    throw new UnsupportedOperationException("Not yet supported");
  }
}
