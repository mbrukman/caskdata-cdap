/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.common.cli.Arguments;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Changes the current namespace.
 */
public class UseNamespaceCommand extends AbstractAuthCommand {

  private final CLIConfig cliConfig;

  @Inject
  public UseNamespaceCommand(CLIConfig cliConfig) {
    super(cliConfig);
    this.cliConfig = cliConfig;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String namespace = arguments.get(ArgumentName.NAMESPACE_ID.toString());
    cliConfig.setCurrentNamespace(namespace);
    output.printf("Now using namespace '%s'\n", namespace);
  }

  @Override
  public String getPattern() {
    return String.format("use namespace <%s>", ArgumentName.NAMESPACE_ID);
  }

  @Override
  public String getDescription() {
    return "Changes the current " + ElementType.NAMESPACE.getPrettyName();
  }
}
