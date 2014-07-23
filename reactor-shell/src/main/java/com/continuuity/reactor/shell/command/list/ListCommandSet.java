/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.reactor.shell.command.list;

import com.continuuity.reactor.client.ReactorAppClient;
import com.continuuity.reactor.client.ReactorDatasetClient;
import com.continuuity.reactor.client.ReactorDatasetModuleClient;
import com.continuuity.reactor.client.ReactorDatasetTypeClient;
import com.continuuity.reactor.client.ReactorStreamClient;
import com.continuuity.reactor.metadata.ProgramType;
import com.continuuity.reactor.shell.command.Command;
import com.continuuity.reactor.shell.command.CommandSet;
import com.google.common.collect.Lists;

import javax.inject.Inject;

/**
 * Contains commands for listing stuff.
 */
public class ListCommandSet extends CommandSet {

  @Inject
  public ListCommandSet(ReactorAppClient appClient, ReactorDatasetClient datasetClient,
                        ReactorStreamClient streamClient,
                        ReactorDatasetModuleClient datasetModuleClient,
                        ReactorDatasetTypeClient datasetTypeClient) {
    super("list",
      new ListAppsCommand(appClient),
      new ListAllProgramsCommand(appClient),
      new ListProgramsCommand(ProgramType.FLOW, appClient),
      new ListProgramsCommand(ProgramType.MAPREDUCE, appClient),
      new ListProgramsCommand(ProgramType.PROCEDURE, appClient),
      new ListProgramsCommand(ProgramType.WORKFLOW, appClient),
      new ListDatasetsCommand(datasetClient),
      new ListDatasetModulesCommand(datasetModuleClient),
      new ListDatasetTypesCommand(datasetTypeClient),
      new ListStreamsCommand(streamClient)
    );
  }
}
