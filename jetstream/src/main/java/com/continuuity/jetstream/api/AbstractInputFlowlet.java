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

package com.continuuity.jetstream.api;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 * Abstract class to implement InputFlowlet.
 */
public abstract class AbstractInputFlowlet extends AbstractFlowlet {
  private InputFlowletConfigurer configurer;

  /**
   * Override this method to configure the InputFlowlet.
   */
  public abstract void create();

  public final void create(InputFlowletConfigurer configurer) {
    this.configurer = configurer;
    create();
  }

  /**
   * Set the name for the InputFlowlet.
   * @param name Name of the InputFlowlet.
   */
  protected void setName(String name) {
    configurer.setName(name);
  }

  /**
   * Set the description for the InputFlowlet.
   * @param description Description of the InputFlowlet.
   */
  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * Add a GDAT Input source to the InputFlowlet.
   * @param name Name of the Input Source.
   * @param schema Schema of the Input Source.
   */
  protected void addGDATInput(String name, StreamSchema schema) {
    configurer.addGDATInput(name, schema);
  }

  /**
   * Add a GSQL query to the InputFlowlet.
   * @param sqlOutName GSQL Query Name (also the name of the Output Stream generated by the Query).
   * @param gsql GSQL query.
   */
  protected void addGSQL(String sqlOutName, String gsql) {
    configurer.addGSQL(sqlOutName, gsql);
  }

}