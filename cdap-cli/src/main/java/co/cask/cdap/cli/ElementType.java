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

package co.cask.cdap.cli;

import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Represents types of programs and their elements.
 */
public enum ElementType {

  NAMESPACE("Namespace", "Namespaces", "namespace", "namespaces",
            null, null, ArgumentName.NAMESPACE_ID),

  APP("application", "applications", "app", "apps",
      null, null, ArgumentName.APP,
      Capability.LIST),

  DATASET("Dataset", "Datasets", "dataset", "datasets",
          null, null, ArgumentName.DATASET,
          Capability.LIST),

  DATASET_MODULE("Dataset module", "Dataset modules", "dataset module", "dataset modules",
                 null, null, ArgumentName.DATASET_MODULE,
                 Capability.LIST),

  DATASET_TYPE("Dataset type", "Dataset types", "dataset type", "dataset types",
               null, null, ArgumentName.DATASET_TYPE,
               Capability.LIST),

  QUERY("Dataset query", "Dataset queries", "dataset query", "dataset queries",
        null, null, ArgumentName.QUERY),

  STREAM("Stream", "Streams", "stream", "streams",
         null, null, ArgumentName.STREAM,
         Capability.LIST),

  PROGRAM("program", "programs", "program", "programs",
          null, null, ArgumentName.PROGRAM),

  FLOW("Flow", "Flows", "flow", "flows",
       ProgramType.FLOW, null,
       ArgumentName.FLOW,
       Capability.RUNS, Capability.LOGS, Capability.LIVE_INFO, Capability.STATUS, Capability.START_STOP,
       Capability.LIST, Capability.RUNTIME_ARGS),

  WORKFLOW("Workflow", "Workflows", "workflow", "workflows",
           ProgramType.WORKFLOW, null,
           ArgumentName.WORKFLOW,
           Capability.RUNS, Capability.STATUS, Capability.START_STOP,
           Capability.LIST, Capability.RUNTIME_ARGS),

  FLOWLET("Flowlet", "Flowlets", "flowlet", "flowlets",
          null, ProgramType.FLOW,
          ArgumentName.FLOWLET,
          Capability.SCALE),

  PROCEDURE("Procedure", "Procedures", "procedure", "procedures",
            ProgramType.PROCEDURE, null,
            ArgumentName.PROCEDURE,
            Capability.RUNS, Capability.SCALE, Capability.LOGS, Capability.LIVE_INFO, Capability.STATUS,
            Capability.START_STOP, Capability.LIST, Capability.RUNTIME_ARGS),

  SERVICE("Service", "Services", "service", "services",
          ProgramType.SERVICE, null,
          ArgumentName.SERVICE,
          Capability.START_STOP, Capability.STATUS, Capability.LIST, Capability.RUNTIME_ARGS),

  RUNNABLE("Runnable", "Runnables", "runnable", "runnables",
           null, ProgramType.SERVICE,
           ArgumentName.RUNNABLE,
           Capability.SCALE, Capability.RUNS, Capability.LOGS),

  MAPREDUCE("MapReduce", "MapReduce Programs", "mapreduce", "mapreduce",
            ProgramType.MAPREDUCE, null,
            ArgumentName.MAPREDUCE,
            Capability.LOGS, Capability.RUNS, Capability.STATUS, Capability.START_STOP, Capability.LIST,
            Capability.RUNTIME_ARGS),

  SPARK("Spark", "Spark Programs", "spark", "spark",
            ProgramType.SPARK, null,
            ArgumentName.SPARK,
            Capability.LOGS, Capability.RUNS, Capability.STATUS, Capability.START_STOP, Capability.LIST,
            Capability.RUNTIME_ARGS);

  private final String pluralName;
  private final String pluralPrettyName;
  private final String name;
  private final ProgramType programType;
  private final ProgramType parentType;
  private final Set<Capability> capabilities;
  private final String prettyName;
  private final ArgumentName argumentName;

  ElementType(String prettyName, String pluralPrettyName,
              String name, String pluralName,
              ProgramType programType, ProgramType parentType,
              ArgumentName argumentName,
              Capability... capabilities) {
    this.prettyName = prettyName;
    this.pluralPrettyName = pluralPrettyName;
    this.name = name;
    this.pluralName = pluralName;
    this.programType = programType;
    this.parentType = parentType;
    this.argumentName = argumentName;
    this.capabilities = Sets.newHashSet(capabilities);
  }

  public boolean isTopLevel() {
    return parentType == null;
  }

  public String getPrettyName() {
    return prettyName;
  }

  public String getName() {
    return name;
  }

  public ArgumentName getArgumentName() {
    return argumentName;
  }

  public String getPluralName() {
    return pluralName;
  }

  public ProgramType getProgramType() {
    return programType;
  }

  public ProgramType getParentType() {
    return parentType;
  }

  public String getPluralPrettyName() {
    return pluralPrettyName;
  }

  public boolean canScale() {
    return capabilities.contains(Capability.SCALE);
  }

  public boolean hasRuns() {
    return capabilities.contains(Capability.RUNS);
  }

  public boolean hasLogs() {
    return capabilities.contains(Capability.LOGS);
  }

  public boolean hasLiveInfo() {
    return capabilities.contains(Capability.LIVE_INFO);
  }

  public boolean hasStatus() {
    return capabilities.contains(Capability.STATUS);
  }

  public boolean canStartStop() {
    return capabilities.contains(Capability.START_STOP);
  }

  public static ElementType fromProgramType(ProgramType programType) {
    for (ElementType elementType : ElementType.values()) {
      if (elementType.getProgramType() == programType) {
        return elementType;
      }
    }
    throw new IllegalArgumentException("Invalid ElementType from ProgramType " + programType);
  }

  public boolean isListable() {
    return capabilities.contains(Capability.LIST);
  }

  public boolean hasRuntimeArgs() {
    return capabilities.contains(Capability.RUNTIME_ARGS);
  }

  private enum Capability {
    SCALE, RUNS, LOGS, LIVE_INFO, STATUS, START_STOP, LIST, RUNTIME_ARGS
  }
}
