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

package co.cask.cdap.spark.stream;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

/**
 * Test Spark program integration with Streams
 */
@Category(XSlowTests.class)
public class SparkStreamIntegrationTestRun extends TestFrameworkTestBase {

  @Test
  public void testSparkWithStream() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestSparkStreamIntegrationApp.class);
    StreamWriter writer = applicationManager.getStreamWriter("testStream");
    for (int i = 0; i < 50; i++) {
      writer.send(String.valueOf(i));
    }

    SparkManager sparkManager = applicationManager.startSpark("SparkStreamProgram");
    sparkManager.waitForFinish(120, TimeUnit.SECONDS);

    // The Spark job simply turns every stream event body into key/value pairs, with key==value.
    DataSetManager<KeyValueTable> datasetManager = applicationManager.getDataSet("result");
    KeyValueTable results = datasetManager.get();
    for (int i = 0; i < 50; i++) {
      byte[] key = String.valueOf(i).getBytes(Charsets.UTF_8);
      Assert.assertArrayEquals(key, results.read(key));
    }
  }
}
