/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.reactor.client;

import com.continuuity.reactor.client.app.FakeApp;
import com.continuuity.reactor.client.app.FakeFlow;
import com.continuuity.reactor.client.app.FakeProcedure;
import com.continuuity.reactor.client.common.ReactorClientTestBase;
import com.continuuity.reactor.client.config.ReactorClientConfig;
import com.continuuity.reactor.metadata.ProgramType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ReactorProgramClientTest extends ReactorClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(ReactorProgramClientTest.class);

  private ReactorAppClient appClient;
  private ReactorProcedureClient procedureClient;
  private ReactorProgramClient programClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();

    ReactorClientConfig config = new ReactorClientConfig("localhost");
    appClient = new ReactorAppClient(config);
    procedureClient = new ReactorProcedureClient(config);
    programClient = new ReactorProgramClient(config);
  }

  @Test
  public void testAll() throws Exception {
    appClient.deploy(createAppJarFile(FakeApp.class));

    // start, scale, and stop procedure
    LOG.info("Fetching procedure list");
    verifyProgramNames(FakeApp.PROCEDURES, procedureClient.list());

    LOG.info("Starting procedure");
    programClient.start(FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);
    assertProgramRunning(programClient, FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);

    LOG.info("Getting live info");
    programClient.getLiveInfo(FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);

    LOG.info("Getting program logs");
    programClient.getProgramLogs(FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME, 0, Long.MAX_VALUE);

    LOG.info("Scaling procedure");
    Assert.assertEquals(1, programClient.getProcedureInstances(FakeApp.NAME, FakeProcedure.NAME));
    programClient.setProcedureInstances(FakeApp.NAME, FakeProcedure.NAME, 3);
    assertProcedureInstances(programClient, FakeApp.NAME, FakeProcedure.NAME, 3);

    LOG.info("Stopping procedure");
    programClient.stop(FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);
    assertProgramStopped(programClient, FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);

    // start, scale, and stop flow
    verifyProgramNames(FakeApp.FLOWS, appClient.listPrograms(FakeApp.NAME, ProgramType.FLOW));

    LOG.info("Starting flow");
    programClient.start(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);
    assertProgramRunning(programClient, FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);

    LOG.info("Getting flow history");
    programClient.getProgramHistory(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);

    LOG.info("Scaling flowlet");
    Assert.assertEquals(1, programClient.getFlowletInstances(FakeApp.NAME, FakeFlow.NAME, FakeFlow.FLOWLET_NAME));
    programClient.setFlowletInstances(FakeApp.NAME, FakeFlow.NAME, FakeFlow.FLOWLET_NAME, 3);
    assertFlowletInstances(programClient, FakeApp.NAME, FakeFlow.NAME, FakeFlow.FLOWLET_NAME, 3);

    LOG.info("Stopping flow");
    programClient.stop(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);
    assertProgramStopped(programClient, FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);

  }
}