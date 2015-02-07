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

package co.cask.cdap.common.exception;

import co.cask.cdap.proto.ProgramType;

/**
 * Thrown when a program is not found
 */
public class ProgramNotFoundException extends NotFoundException {

  private ProgramType programType;
  private String appId;
  private String programId;

  public ProgramNotFoundException(ProgramType programType, String appId, String programId) {
    super(programType.getCategoryName(), appId + "/" + programId);
    this.programType = programType;
    this.appId = appId;
    this.programId = programId;
  }

  /**
   * @return Type of the program that was not found
   */
  public ProgramType getProgramType() {
    return programType;
  }

  /**
   * @return Application ID of the application that the program that was not found belongs to
   */
  public String getAppId() {
    return appId;
  }

  /**
   * @return ID of the program that was not found
   */
  public String getProgramId() {
    return programId;
  }
}
