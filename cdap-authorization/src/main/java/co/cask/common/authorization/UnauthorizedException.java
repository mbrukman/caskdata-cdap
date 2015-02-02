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
package co.cask.common.authorization;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

/**
 * Thrown when a user tries to access a protected resource without the necessary permissions.
 */
public class UnauthorizedException extends RuntimeException {

  private final ImmutableList<ObjectId> objects;
  private final ImmutableList<SubjectId> subjects;
  private final ImmutableList<Permission> requiredPermissions;

  public UnauthorizedException(Iterable<ObjectId> objects, Iterable<SubjectId> subjects,
                               Iterable<Permission> requiredPermissions) {
    super(generateMessage(objects, subjects, requiredPermissions));
    this.objects = ImmutableList.copyOf(objects);
    this.subjects = ImmutableList.copyOf(subjects);
    this.requiredPermissions = ImmutableList.copyOf(requiredPermissions);
  }

  private static String generateMessage(Iterable<ObjectId> objects, Iterable<SubjectId> subjects,
                                        Iterable<Permission> requiredPermissions) {
    Joiner joiner = Joiner.on(", ");
    return "None of the subjects (" + joiner.join(subjects) + ") are authorized to access any of the objects ("
      + joiner.join(objects) + ") due to missing one or more of the required permissions ("
      + joiner.join(requiredPermissions) + ")";
  }

  public ImmutableList<ObjectId> getObjects() {
    return objects;
  }

  public ImmutableList<SubjectId> getSubjects() {
    return subjects;
  }

  public ImmutableList<Permission> getRequiredPermissions() {
    return requiredPermissions;
  }
}
