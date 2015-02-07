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
package co.cask.cdap.gateway.handlers.metrics;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollector;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MetricsDeleteTestRun extends MetricsSuiteTestBase {

  @Test
  public void testContextDelete() throws Exception {
    // Insert some metrics
    MetricsCollector collector =
      collectionService.getCollector(getFlowletContext(Constants.DEFAULT_NAMESPACE, "WCount", "WordCounter", "unique"));
    collector.increment("process.events.processed", 6);
    collector.increment("process.events.out", 5);

    collector = collectionService.getCollector(getFlowletContext(Constants.DEFAULT_NAMESPACE, "WCount", "WordCounter",
                                                                 "counter"));
    collector.increment("process.events.processed", 4);
    collector.increment("process.events.out", 3);

    collector = collectionService.getCollector(getFlowletContext(Constants.DEFAULT_NAMESPACE, "WCount", "WCounter",
                                                                 "counter"));
    collector.increment("process.events.processed", 2);
    collector.increment("process.events.out", 1);

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    String base = "/v2/metrics/system/apps/WCount/flows";
    // make sure data is there
    Assert.assertEquals(6, getMetricCount(base + "/WordCounter/flowlets/unique", "process.events.processed"));
    Assert.assertEquals(5, getMetricCount(base + "/WordCounter/flowlets/unique", "process.events.out"));
    Assert.assertEquals(4, getMetricCount(base + "/WordCounter/flowlets/counter", "process.events.processed"));
    Assert.assertEquals(3, getMetricCount(base + "/WordCounter/flowlets/counter", "process.events.out"));
    Assert.assertEquals(2, getMetricCount(base + "/WCounter/flowlets/counter", "process.events.processed"));
    Assert.assertEquals(1, getMetricCount(base + "/WCounter/flowlets/counter", "process.events.out"));

    // do the delete
    HttpResponse response = doDelete(base + "/WordCounter");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    // test correct metrics got deleted
    Assert.assertEquals(0, getMetricCount(base + "/WordCounter/flowlets/unique", "process.events.processed"));
    Assert.assertEquals(0, getMetricCount(base + "/WordCounter/flowlets/unique", "process.events.out"));
    Assert.assertEquals(0, getMetricCount(base + "/WordCounter/flowlets/counter", "process.events.processed"));
    Assert.assertEquals(0, getMetricCount(base + "/WordCounter/flowlets/counter", "process.events.out"));
    // test other things did not get deleted
    Assert.assertEquals(2, getMetricCount(base + "/WCounter/flowlets/counter", "process.events.processed"));
    Assert.assertEquals(1, getMetricCount(base + "/WCounter/flowlets/counter", "process.events.out"));
  }

  @Test
  public void testContextAndMetricDelete() throws Exception {
    // Insert some metrics
    MetricsCollector collector =
      collectionService.getCollector(getFlowletContext(Constants.DEFAULT_NAMESPACE, "WCount", "WordCounter", "unique"));
    collector.increment("process.events.processed", 6);
    collector.increment("process.events.out", 5);
    collector.increment("store.ops", 7);

    collector =
      collectionService.getCollector(getFlowletContext(Constants.DEFAULT_NAMESPACE, "WCount", "WordCounter",
                                                       "counter"));
    collector.increment("process.events.processed", 4);
    collector.increment("process.events.out", 3);

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    String base = "/v2/metrics/system/apps/WCount/flows/WordCounter";
    // make sure data is there
    Assert.assertEquals(6, getMetricCount(base + "/flowlets/unique", "process.events.processed"));
    Assert.assertEquals(5, getMetricCount(base + "/flowlets/unique", "process.events.out"));
    Assert.assertEquals(7, getMetricCount(base + "/flowlets/unique", "store.ops"));
    Assert.assertEquals(4, getMetricCount(base + "/flowlets/counter", "process.events.processed"));
    Assert.assertEquals(3, getMetricCount(base + "/flowlets/counter", "process.events.out"));

    // do the delete
    HttpResponse response = doDelete(base + "/flowlets/unique?prefixEntity=process");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    // test correct metrics got deleted
    Assert.assertEquals(0, getMetricCount(base + "/flowlets/unique", "process.events.processed"));
    Assert.assertEquals(0, getMetricCount(base + "/flowlets/unique", "process.events.out"));
    // test other things did not get deleted
    Assert.assertEquals(7, getMetricCount(base + "/flowlets/unique", "store.ops"));
    Assert.assertEquals(4, getMetricCount(base + "/flowlets/counter", "process.events.processed"));
    Assert.assertEquals(3, getMetricCount(base + "/flowlets/counter", "process.events.out"));
  }

  @Test
  public void testMetricNoContextDelete() throws Exception {
    // Insert some metrics
    MetricsCollector collector =
      collectionService.getCollector(getFlowletContext(Constants.DEFAULT_NAMESPACE, "WCount", "WordCounter", "unique"));
    collector.increment("store.ops", 7);
    collector.increment("process.events.processed", 6);
    collector.increment("process.events.out", 5);

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    String base = "/v2/metrics/system";
    // make sure data is there
    Assert.assertEquals(7, getMetricCount(base, "store.ops"));
    Assert.assertEquals(6, getMetricCount(base, "process.events.processed"));
    Assert.assertEquals(5, getMetricCount(base, "process.events.out"));

    // do the delete
    HttpResponse response = doDelete(base + "?prefixEntity=process");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    // test correct metrics got deleted
    Assert.assertEquals(0, getMetricCount(base, "process.events.processed"));
    Assert.assertEquals(0, getMetricCount(base, "process.events.out"));
    // test other things did not get deleted
    Assert.assertEquals(7, getMetricCount(base, "store.ops"));
  }

  @Test
  public void testNonExistingPathSucceeds() throws Exception {
    for (String resource : nonExistingResources) {
      // strip metric name from end of resource since delete handler doesn't have that in the path
      resource = resource.substring(0, resource.lastIndexOf("/"));
      // test GET request fails with 404
      HttpResponse response = doDelete("/v2/metrics" + resource);
      Assert.assertEquals("DELETE " + resource + " did not return 200 as expected.",
                          HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }
  }

  private int getMetricCount(String path, String metric) throws Exception {
    HttpResponse response = doGet(path + "/" + metric + "?aggregate=true");
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    return new Gson().fromJson(reader, JsonObject.class).get("data").getAsInt();
  }

  @After
  public void clearMetrics() throws Exception {
    doDelete("/v2/metrics");
  }
}
