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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

/**
 * Provides methods for storing and querying {@link ACLEntry}s.
 */
public interface ACLStore {

  /**
   * Writes a single {@link ACLEntry}.
   *
   *
   * @param entry the {@link ACLEntry} to write
   */
  void write(ACLEntry entry) throws Exception;

  /**
   * Checks for the existence of an {@link ACLEntry}.
   *
   *
   * @param entry the {@link ACLEntry} to check
   */
  void exists(ACLEntry entry) throws Exception;

  /**
   * Deletes an {@link ACLEntry} matching.
   *
   *
   * @param entry the {@link ACLEntry} to delete
   */
  void delete(ACLEntry entry) throws Exception;

  /**
   * Fetches {@link ACLEntry}s matching the specified {@link Query}.
   *
   * @param query specifies the {@link ACLEntry}s to read
   * @return the {@link ACLEntry}s that have the {@code object}.
   */
  Set<ACLEntry> search(Query query) throws Exception;

  /**
   * Deletes {@link ACLEntry}s matching the specified {@link Query}.
   *
   * @param query specifies the {@link ACLEntry}s to delete
   */
  void delete(Query query) throws Exception;

  /**
   * Represents a query for searching existing {@link ACLEntry}s. If any conditions are satisfied
   * for an {@link ACLEntry}, then the {@link ACLEntry} will be included in the results.
   */
  public static final class Query {

    private final List<Condition> conditions;

    public Query(Condition... conditions) {
      this.conditions = ImmutableList.copyOf(conditions);
    }

    public Query(List<Condition> conditions) {
      this.conditions = ImmutableList.copyOf(conditions);
    }

    public Query(ObjectId objectId, SubjectId subject, Permission permission) {
      this.conditions = ImmutableList.of(new Condition(objectId, subject, permission));
    }

    public Query(ObjectId objectId, SubjectId subject) {
      this.conditions = ImmutableList.of(new Condition(objectId, subject));
    }

    public Query(ACLEntry aclEntry) {
      this.conditions = ImmutableList.of(new Condition(aclEntry));
    }

    public List<Condition> getConditions() {
      return conditions;
    }
  }

  /**
   * Represents a condition to match when searching for ACLs.
   */
  public static final class Condition {

    private final Optional<ObjectId> objectId;
    private final Optional<SubjectId> subjectId;
    private final Optional<Permission> permission;

    public Condition(ObjectId objectId, SubjectId subjectId, Permission permission) {
      this.objectId = Optional.fromNullable(objectId);
      this.subjectId = Optional.fromNullable(subjectId);
      this.permission = Optional.fromNullable(permission);
    }

    public Condition(ObjectId objectId, SubjectId subjectId) {
      this(objectId, subjectId, null);
    }

    public Condition(ACLEntry entry) {
      this(entry.getObject(), entry.getSubject(), entry.getPermission());
    }

    public Optional<ObjectId> getObjectId() {
      return objectId;
    }

    public Optional<SubjectId> getSubjectId() {
      return subjectId;
    }

    public Optional<Permission> getPermission() {
      return permission;
    }

    /**
     * @param entry the entry to check
     * @return true if entry matches this condition
     */
    public boolean matches(ACLEntry entry) {
      return !(objectId.isPresent() && !objectId.get().equals(entry.getObject()))
        && !(subjectId.isPresent() && !subjectId.get().equals(entry.getSubject()))
        && !(permission.isPresent() && !permission.get().equals(entry.getPermission()));

    }
  }
}
