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
package co.cask.cdap.metrics.query;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.metrics.data.Interpolators;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MetricsRequestParserTest {

  @Test
  public void testPathStrip() {
    String expected = "/system/apps/app1/flows/flow1/metric?aggregate=true";
    String path = Constants.Gateway.API_VERSION_2 + "/metrics" + expected;
    Assert.assertEquals(expected, MetricsRequestParser.stripVersionAndMetricsFromPath(path));
  }

  @Test
  public void testQueryArgs() throws MetricsPathException {
    MetricsRequest request = MetricsRequestParser.parse(URI.create("/system/apps/app1/reads?count=60"));
    Assert.assertEquals(MetricsRequest.Type.TIME_SERIES, request.getType());
    Assert.assertEquals(60, request.getCount());

    request = MetricsRequestParser.parse(URI.create("/system/apps/app1/reads?summary=true"));
    Assert.assertEquals(MetricsRequest.Type.SUMMARY, request.getType());

    request = MetricsRequestParser.parse(URI.create("/system/apps/app1/reads?aggregate=true"));
    Assert.assertEquals(MetricsRequest.Type.AGGREGATE, request.getType());

    request = MetricsRequestParser.parse(URI.create("/system/apps/app1/reads?count=60&start=1&end=61&" +
                                                      "resolution=1s"));
    Assert.assertEquals(1, request.getStartTime());
    Assert.assertEquals(61, request.getEndTime());
    Assert.assertEquals(MetricsRequest.TimeSeriesResolution.SECOND.getResolution(), request.getTimeSeriesResolution());
    Assert.assertEquals(MetricsRequest.Type.TIME_SERIES, request.getType());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/reads?count=60&start=1&end=61&resolution=1m"));
    Assert.assertEquals(1, request.getStartTime());
    Assert.assertEquals(61, request.getEndTime());
    Assert.assertEquals(MetricsRequest.Type.TIME_SERIES, request.getType());
    Assert.assertEquals(MetricsRequest.TimeSeriesResolution.MINUTE.getResolution(), request.getTimeSeriesResolution());
    Assert.assertNull(request.getInterpolator());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/reads?start=1&end=61&resolution=60m"));
    Assert.assertEquals(1, request.getStartTime());
    Assert.assertEquals(61, request.getEndTime());
    Assert.assertEquals(MetricsRequest.Type.TIME_SERIES, request.getType());
    Assert.assertEquals(MetricsRequest.TimeSeriesResolution.HOUR.getResolution(), request.getTimeSeriesResolution());
    Assert.assertNull(request.getInterpolator());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/reads?count=60&start=1&end=61&interpolate=step"));
    Assert.assertEquals(1, request.getStartTime());
    Assert.assertEquals(61, request.getEndTime());
    Assert.assertEquals(MetricsRequest.Type.TIME_SERIES, request.getType());
    Assert.assertTrue(request.getInterpolator() instanceof Interpolators.Step);

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/reads?count=60&start=1&end=61&interpolate=linear"));
    Assert.assertEquals(1, request.getStartTime());
    Assert.assertEquals(61, request.getEndTime());
    Assert.assertEquals(MetricsRequest.Type.TIME_SERIES, request.getType());
    Assert.assertTrue(request.getInterpolator() instanceof Interpolators.Linear);
  }

  @Test
  public void testRelativeTimeArgs() throws MetricsPathException  {
    long now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/reads?count=61&end=now-5s"));
    assertTimestamp(now - 5, request.getEndTime());
    assertTimestamp(now - 65, request.getStartTime());

    now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/reads?count=61&start=now-65s"));
    assertTimestamp(now - 5, request.getEndTime());
    assertTimestamp(now - 65, request.getStartTime());

    now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/reads?count=61&start=now-1m"));
    assertTimestamp(now, request.getEndTime());
    assertTimestamp(now - 60, request.getStartTime());

    now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/reads?count=61&start=now-1h"));
    assertTimestamp(now - 3600 + 60, request.getEndTime());
    assertTimestamp(now - 3600, request.getStartTime());

    now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/reads?count=61&start=now-1d"));
    assertTimestamp(now - 86400 + 60, request.getEndTime());
    assertTimestamp(now - 86400, request.getStartTime());

    now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/reads?count=61&start=now-1m&end=now"));
    assertTimestamp(now, request.getEndTime());
    assertTimestamp(now - 60, request.getStartTime());

    now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/reads?count=61&start=now-2m%2B20s"));
    assertTimestamp(now - 40, request.getEndTime());
    assertTimestamp(now - 100, request.getStartTime());
  }

  // assuming you got the actual timestamp after the expected, check that they are equal,
  // or that the actual is 1 second before the expected in case we were on a second boundary.
  private void assertTimestamp(long expected, long actual) {
    Assert.assertTrue(actual + " not within 1 second of " + expected, expected == actual || (actual - 1) == expected);
  }

  @Test
  public void testOverview() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(URI.create("/system/reads?aggregate=true"));
    Assert.assertNull(request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());
  }

  @Test
  public void testApps() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(URI.create("/system/apps/app1/reads?aggregate=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1", request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());
  }

  @Test
  public void testFlow() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/flows/flow1/flowlets/flowlet1/process.bytes?count=60&start=1&end=61"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("system.process.bytes", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/flows/flow1/some.metric?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f.flow1", request.getContextPrefix());
    Assert.assertEquals("system.some.metric", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/flows/loads?aggregate=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f", request.getContextPrefix());
    Assert.assertEquals("system.loads", request.getMetricPrefix());

    //flow with runId
    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/flows/flow1/runs/1234/some.metric?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f.flow1", request.getContextPrefix());
    Assert.assertEquals("system.some.metric", request.getMetricPrefix());
    Assert.assertEquals("1234", request.getRunId());

    //flowlet with runId
    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/flows/flow1/runs/1234/flowlets/flowlet1/some.metric?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("system.some.metric", request.getMetricPrefix());
    Assert.assertEquals("1234", request.getRunId());
  }

  @Test(expected = MetricsPathException.class)
  public void testMultipleRunIdInvalidPath() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/flows/flow1/runs/1234/runs/1235/flowlets/flowlet1/some.metric?summary=true"));
    Assert.assertEquals("app1.f.flow1.flowlet1", request.getContextPrefix());
  }

  @Test
  public void testQueues() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/flows/flow1/flowlets/flowlet1/queues/queue1/process.bytes.in?aggregate=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("system.process.bytes.in", request.getMetricPrefix());
    Assert.assertEquals("queue1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/flows/flow1/flowlets/flowlet1/queues/queue1/process.bytes.out?aggregate=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("system.process.bytes.out", request.getMetricPrefix());
    Assert.assertEquals("queue1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/flows/flow1/flowlets/flowlet1/queues/queue1/process.events.in?aggregate=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("system.process.events.in", request.getMetricPrefix());
    Assert.assertEquals("queue1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/flows/flow1/flowlets/flowlet1/queues/queue1/process.events.out?aggregate=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("system.process.events.out", request.getMetricPrefix());
    Assert.assertEquals("queue1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/flows/flow1/runs/run123/flowlets/flowlet1/queues/queue1/" +
                   "process.events.out?aggregate=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("system.process.events.out", request.getMetricPrefix());
    Assert.assertEquals("queue1", request.getTagPrefix());
    Assert.assertEquals("run123", request.getRunId());
  }

  @Test
  public void testMapReduce() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/mapreduce/mapred1/mappers/reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.b.mapred1.m", request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/mapreduce/mapred1/reducers/reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.b.mapred1.r", request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/mapreduce/mapred1/reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.b.mapred1", request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/mapreduce/reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.b", request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/mapreduce/mapred1/runs/run123/reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.b.mapred1", request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());
    Assert.assertEquals("run123", request.getRunId());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/mapreduce/mapred1/runs/run123/mappers/reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.b.mapred1.m", request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());
    Assert.assertEquals("run123", request.getRunId());
  }

  @Test
  public void testProcedure() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/procedures/proc1/reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.p.proc1", request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/procedures/reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.p", request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/procedures/proc1/runs/run123/reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.p.proc1", request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());
    Assert.assertEquals("run123", request.getRunId());
  }

  @Test
  public void testUserServices() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/services/serve1/reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.u.serve1", request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/services/serve1/runnables/run1/reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.u.serve1.run1", request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/services/serve1/runs/runid123/runnables/run1/reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.u.serve1.run1", request.getContextPrefix());
    Assert.assertEquals("system.reads", request.getMetricPrefix());
    Assert.assertEquals("runid123", request.getRunId());
  }

  @Test
  public void testSpark() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/spark/fakespark/sparkmetric?aggregate=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.s.fakespark", request.getContextPrefix());
    Assert.assertEquals("system.sparkmetric", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/spark/fakespark/runs/runid123/sparkmetric?aggregate=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.s.fakespark", request.getContextPrefix());
    Assert.assertEquals("system.sparkmetric", request.getMetricPrefix());
    Assert.assertEquals("runid123", request.getRunId());
  }


  @Test(expected = MetricsPathException.class)
  public void testInvalidUserServices() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/service/serve1/reads?summary=true"));
  }

  @Test(expected = MetricsPathException.class)
  public void testInvalidUserServicesTooManyPath() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/apps/app1/services/serve1/runnables/run1/random/reads?summary=true"));
  }

  @Test
  public void testDataset() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/datasets/dataset1/apps/app1/flows/flow1/runs/run1/" +
                   "flowlets/flowlet1/store.reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("system.store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());
    Assert.assertEquals("run1", request.getRunId());

    request = MetricsRequestParser.parse(
      URI.create("/system/datasets/dataset1/apps/app1/flows/flow1/store.reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f.flow1", request.getContextPrefix());
    Assert.assertEquals("system.store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/datasets/dataset1/apps/app1/flows/flow1/runs/123/store.reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f.flow1", request.getContextPrefix());
    Assert.assertEquals("system.store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());
    Assert.assertEquals("123", request.getRunId());

    request = MetricsRequestParser.parse(
      URI.create("/system/datasets/dataset1/apps/app1/flows/store.reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f", request.getContextPrefix());
    Assert.assertEquals("system.store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/datasets/dataset1/apps/app1/store.reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1", request.getContextPrefix());
    Assert.assertEquals("system.store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/system/datasets/dataset1/store.reads?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE, request.getContextPrefix());
    Assert.assertEquals("system.store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());
  }

  @Test
  public void testStream() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/streams/stream1/collect.events?summary=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE, request.getContextPrefix());
    Assert.assertEquals("system.collect.events", request.getMetricPrefix());
    Assert.assertEquals("stream1", request.getTagPrefix());
  }


  @Test
  public void testService() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/services/appfabric/request.received?aggregate=true"));
    Assert.assertEquals("system.appfabric", request.getContextPrefix());
    Assert.assertEquals("system.request.received", request.getMetricPrefix());
  }


  @Test
  public void testHandler() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/services/appfabric/handlers/AppFabricHttpHandler/runs/123/" +
                   "response.server-error?aggregate=true"));
    Assert.assertEquals("system.appfabric.AppFabricHttpHandler", request.getContextPrefix());
    Assert.assertEquals("system.response.server-error", request.getMetricPrefix());
    Assert.assertEquals("123", request.getRunId());
  }

  @Test
  public void testMethod() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/services/metrics/handlers/MetricsQueryHandler/methods/handleComponent/" +
                   "response.successful?aggregate=true"));
    Assert.assertEquals("system.metrics.MetricsQueryHandler.handleComponent", request.getContextPrefix());
    Assert.assertEquals("system.response.successful", request.getMetricPrefix());
  }

  @Test(expected = MetricsPathException.class)
  public void testInvalidRequest() throws MetricsPathException {
    //handler instead of handlers
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/services/metrics/handler/MetricsQueryHandler/" +
                   "response.successful?aggregate=true"));
  }

  @Test
  public void testCluster() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/cluster/resources.total.storage?count=1&start=12345678&interpolate=step"));
    Assert.assertEquals("system.-.cluster", request.getContextPrefix());
    Assert.assertEquals("system.resources.total.storage", request.getMetricPrefix());
  }


  @Test
  public void testTransactions() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/system/transactions/invalid?count=1&start=12345678&interpolate=step"));
    Assert.assertEquals("system.transactions", request.getContextPrefix());
    Assert.assertEquals("system.invalid", request.getMetricPrefix());
  }

  @Test
  public void testMetricURIDecoding() throws UnsupportedEncodingException, MetricsPathException {
    String weirdMetric = "/weird?me+tr ic#$name////";
    // encoded version or weirdMetric
    String encodedWeirdMetric = "%2Fweird%3Fme%2Btr%20ic%23%24name%2F%2F%2F%2F";
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/user/apps/app1/flows/" + encodedWeirdMetric + "?aggregate=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1.f", request.getContextPrefix());
    Assert.assertEquals("user." + weirdMetric, request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/user/apps/app1/" + encodedWeirdMetric + "?aggregate=true"));
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE + ".app1", request.getContextPrefix());
    Assert.assertEquals("user." + weirdMetric, request.getMetricPrefix());
  }


  @Test(expected = IllegalArgumentException.class)
  public void testUserMetricBadURIThrowsException() throws MetricsPathException {
    String badEncoding = "/%2";
    MetricsRequestParser.parse(URI.create("/user/apps/app1/flows" + badEncoding + "?aggregate=true"));
  }

  @Test
  public void testBadPathsThrowExceptions() {
    int numBad = 0;
    String[] validPaths = {
      "/system/metric?aggregate=true",
      "/system/apps/appX/metric?aggregate=true",
      "/system/apps/appX/flows/metric?aggregate=true",
      "/system/apps/appX/flows/flowY/metric?aggregate=true",
      "/system/apps/appX/flows/flowY/flowlets/flowletZ/metric?aggregate=true",
      "/system/apps/appX/procedures/metric?aggregate=true",
      "/system/apps/appX/procedures/procedureY/metric?aggregate=true",
      "/system/apps/appX/mapreduce/metric?aggregate=true",
      "/system/apps/appX/mapreduce/mapreduceY/metric?aggregate=true",
      "/system/apps/appX/mapreduce/mapreduceY/mappers/metric?aggregate=true",
      "/system/apps/appX/mapreduce/mapreduceY/reducers/metric?aggregate=true",
      "/system/datasets/datasetA/metric?aggregate=true",
      "/system/datasets/datasetA/apps/appX/metric?aggregate=true",
      "/system/datasets/datasetA/apps/appX/flows/flowY/metric?aggregate=true",
      "/system/datasets/datasetA/apps/appX/flows/flowY/flowlets/flowletZ/metric?aggregate=true",
      "/system/streams/streamA/metric?aggregate=true"
    };
    // check that miss-spelled paths and the like throw an exception.
    String[] invalidPaths = {
      "/syste/metric?aggregate=true",
      "/system/app/appX/metric?aggregate=true",
      "/system/apps/appX/flow/metric?aggregate=true",
      "/system/apps/appX/flows/flowY/flowlet/flowletZ/metric?aggregate=true",
      "/system/apps/appX/procedure/metric?aggregate=true",
      "/system/apps/appX/procedure/procedureY/metric?aggregate=true",
      "/system/apps/appX/mapreduces/metric?aggregate=true",
      "/system/apps/appX/mapreduces/mapreduceY/metric?aggregate=true",
      "/system/apps/appX/mapreduce/mapreduceY/mapper/metric?aggregate=true",
      "/system/apps/appX/mapreduce/mapreduceY/reducer/metric?aggregate=true",
      "/system/dataset/datasetA/metric?aggregate=true",
      "/system/datasets/datasetA/app/appX/metric?aggregate=true",
      "/system/datasets/datasetA/apps/appX/flow/flowY/metric?aggregate=true",
      "/system/datasets/datasetA/apps/appX/flows/flowY/flowlet/flowletZ/metric?aggregate=true",
      "/system/stream/streamA/metric?aggregate=true"
    };
    for (String path : validPaths) {
      try {
        MetricsRequest request = MetricsRequestParser.parse(URI.create(path));
      } catch (MetricsPathException e) {
        numBad++;
      }
    }
    Assert.assertEquals(0, numBad);
    for (String path : invalidPaths) {
      try {
        MetricsRequest request = MetricsRequestParser.parse(URI.create(path));
      } catch (MetricsPathException e) {
        numBad++;
      }
    }
    Assert.assertEquals(invalidPaths.length, numBad);
  }
}
