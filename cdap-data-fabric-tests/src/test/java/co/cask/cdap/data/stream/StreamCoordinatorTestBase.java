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
package co.cask.cdap.data.stream;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class StreamCoordinatorTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  protected abstract StreamCoordinatorClient createStreamCoordinator();

  @Test
  public void testGeneration() throws ExecutionException, InterruptedException, IOException {
    final StreamCoordinatorClient coordinator = createStreamCoordinator();
    coordinator.startAndWait();

    final CountDownLatch genIdChanged = new CountDownLatch(1);
    final Id.Stream streamId = Id.Stream.from(Constants.DEFAULT_NAMESPACE, "testGen");
    coordinator.addListener(streamId, new StreamPropertyListener() {
      @Override
      public void generationChanged(Id.Stream streamName, int generation) {
        if (generation == 10) {
          genIdChanged.countDown();
        }
      }
    });

    // Do concurrent calls to nextGeneration using two threads
    final CyclicBarrier barrier = new CyclicBarrier(2);
    for (int i = 0; i < 2; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            barrier.await();
            for (int i = 0; i < 5; i++) {
              coordinator.nextGeneration(createStreamConfig(streamId), 0);
            }
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };
      t.start();
    }

    Assert.assertTrue(genIdChanged.await(10, TimeUnit.SECONDS));

    coordinator.stopAndWait();
  }

  private StreamConfig createStreamConfig(Id.Stream stream) throws IOException {
    return new StreamConfig(stream.getId(), 3600000, 10000, Long.MAX_VALUE,
                            new LocalLocationFactory(tmpFolder.newFolder()).create(stream.getId()),
                            null, 1000);
  }
}
