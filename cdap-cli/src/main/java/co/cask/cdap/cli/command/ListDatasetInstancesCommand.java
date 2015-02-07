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

package co.cask.cdap.cli.command;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.AsciiTable;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.client.DatasetClient;
import co.cask.common.cli.Arguments;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * Lists datasets.
 */
public class ListDatasetInstancesCommand extends AbstractAuthCommand {

  private final DatasetClient datasetClient;

  @Inject
  public ListDatasetInstancesCommand(DatasetClient datasetClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.datasetClient = datasetClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    List<DatasetSpecification> datasetMetas = datasetClient.list();

    new AsciiTable<DatasetSpecification>(
      new String[]{"name", "type"}, datasetMetas,
      new RowMaker<DatasetSpecification>() {
        @Override
        public Object[] makeRow(DatasetSpecification object) {
          return new Object[] { object.getName(), object.getType() };
        }
      }).print(output);
  }

  @Override
  public String getPattern() {
    return "list dataset instances";
  }

  @Override
  public String getDescription() {
    return "Lists all " + ElementType.DATASET.getPluralPrettyName();
  }
}
