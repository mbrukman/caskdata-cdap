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

package com.continuuity.reactor.shell.command.delete;

import com.continuuity.reactor.client.ReactorDatasetClient;
import com.continuuity.reactor.shell.command.AbstractCommand;
import com.continuuity.reactor.shell.completer.Completable;
import com.continuuity.reactor.shell.completer.reactor.DatasetNameCompleter;
import com.google.common.collect.Lists;
import jline.console.completer.Completer;

import java.io.PrintStream;
import java.util.List;

/**
 * Deletes a dataset.
 */
public class DeleteDatasetCommand extends AbstractCommand implements Completable {

  private final ReactorDatasetClient datasetClient;
  private final DatasetNameCompleter completer;

  protected DeleteDatasetCommand(DatasetNameCompleter completer, ReactorDatasetClient datasetClient) {
    super("dataset", "<dataset-name>", "Deletes a dataset");
    this.completer = completer;
    this.datasetClient = datasetClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String datasetName = args[0];
    datasetClient.delete(datasetName);
  }

  @Override
  public List<? extends Completer> getCompleters(String prefix) {
    return Lists.newArrayList(prefixCompleter(prefix, completer));
  }
}
