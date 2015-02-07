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

package co.cask.cdap.batch.stream;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(XSlowTests.class)
public class BatchStreamIntegrationTestRun extends TestFrameworkTestBase {

  /**
   * TestsMapReduce that consumes from stream using BytesWritableStreamDecoder
   * @throws Exception
   */
  @Test
  public void testStreamBatch() throws Exception {
    submitAndVerifyStreamBatchJob(TestBatchStreamIntegrationApp.class, "s_1", "StreamTestBatch", 300);
  }

  /**
   * Tests MapReduce that consumes from stream using IdentityStreamEventDecoder
   * @throws Exception
   */
  @Test
  public void testStreamBatchIdDecoder() throws Exception {
    submitAndVerifyStreamBatchJob(TestBatchStreamIntegrationApp.class, "s_1", "StreamTestBatchIdDecoder", 300);
  }

  /**
   * Tests MapReduce that consumes from stream without mapper.
   */
  @Test
  public void testNoMapperStreamInput() throws Exception {
    submitAndVerifyStreamBatchJob(NoMapperApp.class, "nomapper", "NoMapperMapReduce", 120);
  }

  private void submitAndVerifyStreamBatchJob(Class<? extends AbstractApplication> appClass, String streamWriter, String
    mapReduceName, int timeout) throws Exception {
    ApplicationManager applicationManager = deployApplication(appClass);
    StreamWriter writer = applicationManager.getStreamWriter(streamWriter);
    for (int i = 0; i < 50; i++) {
      writer.send(String.valueOf(i));
    }

    MapReduceManager mapReduceManager = applicationManager.startMapReduce(mapReduceName);
    mapReduceManager.waitForFinish(timeout, TimeUnit.SECONDS);

    // The MR job simply turns every stream event body into key/value pairs, with key==value.
    DataSetManager<KeyValueTable> datasetManager = applicationManager.getDataSet("results");
    KeyValueTable results = datasetManager.get();
    for (int i = 0; i < 50; i++) {
      byte[] key = String.valueOf(i).getBytes(Charsets.UTF_8);
      Assert.assertArrayEquals(key, results.read(key));
    }
  }
}
