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

package com.continuuity.reactor.shell.command.create;

import com.continuuity.reactor.client.ReactorStreamClient;
import com.continuuity.reactor.shell.command.AbstractCommand;

import java.io.PrintStream;
import javax.inject.Inject;

/**
 * Creates a stream.
 */
public class CreateStreamCommand extends AbstractCommand {

  private final ReactorStreamClient streamClient;

  @Inject
  public CreateStreamCommand(ReactorStreamClient streamClient) {
    super("stream", "<new-stream-id>", "Creates a stream");
    this.streamClient = streamClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String streamId = args[0];
    streamClient.create(streamId);
  }
}