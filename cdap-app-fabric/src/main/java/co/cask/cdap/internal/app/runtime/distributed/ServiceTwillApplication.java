/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.filesystem.Location;

import java.io.File;
import java.util.Map;

/**
 * TwillApplication for service. Used to localize program jar location before running the TwillApplication.
 */
public class ServiceTwillApplication implements TwillApplication {

  private final ServiceSpecification spec;
  private final Program program;
  private final File hConfig;
  private final File cConfig;
  private final EventHandler eventHandler;

  public ServiceTwillApplication(Program program, ServiceSpecification spec, File hConfig, File cConfig,
                                 EventHandler eventHandler) {
    this.program = program;
    this.spec = spec;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
    this.eventHandler = eventHandler;
  }

  @Override
  public TwillSpecification configure() {
    TwillSpecification.Builder.MoreRunnable moreRunnable = TwillSpecification.Builder.with()
      .setName(String.format("%s.%s.%s.%s", ProgramType.SERVICE.name().toLowerCase(), program.getNamespaceId(),
                             program.getApplicationId(), spec.getName()))
      .withRunnable();

    Location programLocation = program.getJarLocation();
    String programName = programLocation.getName();

    // Add a runnable for the service handler
    Resources resources = spec.getResources();
    TwillSpecification.Builder.RunnableSetter runnableSetter;
    runnableSetter = moreRunnable.add(spec.getName(),
                                      new ServiceTwillRunnable(spec.getName(), "hConf.xml", "cConf.xml"),
                                      createResourceSpec(resources, spec.getInstances()))
                                  .withLocalFiles()
                                    .add(programName, programLocation.toURI())
                                    .add("hConf.xml", hConfig.toURI())
                                    .add("cConf.xml", cConfig.toURI()).apply();

    // Add runnables for all workers
    for (Map.Entry<String, ServiceWorkerSpecification> entry : spec.getWorkers().entrySet()) {
      ServiceWorkerSpecification workerSpec = entry.getValue();
      runnableSetter = runnableSetter.add(workerSpec.getName(),
                                          new ServiceTwillRunnable(workerSpec.getName(), "hConf.xml", "cConf.xml"),
                                          createResourceSpec(workerSpec.getResources(), workerSpec.getInstances()))
                                     .withLocalFiles()
                                       .add(programName, programLocation.toURI())
                                       .add("hConf.xml", hConfig.toURI())
                                       .add("cConf.xml", cConfig.toURI()).apply();
    }

    return runnableSetter.anyOrder().withEventHandler(eventHandler).build();
  }

  private ResourceSpecification createResourceSpec(Resources resources, int instances) {
    return ResourceSpecification.Builder.with()
      .setVirtualCores(resources.getVirtualCores())
      .setMemory(resources.getMemoryMB(), ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();
  }
}
