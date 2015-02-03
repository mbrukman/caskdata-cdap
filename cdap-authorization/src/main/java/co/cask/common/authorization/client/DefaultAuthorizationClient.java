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
package co.cask.common.authorization.client;

import co.cask.common.authorization.ACLEntry;
import co.cask.common.authorization.ACLStore;
import co.cask.common.authorization.IdentifiableObject;
import co.cask.common.authorization.ObjectId;
import co.cask.common.authorization.Permission;
import co.cask.common.authorization.SubjectId;
import co.cask.common.authorization.UnauthorizedException;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link AuthorizationClient} which uses {@link ACLStore}.
 */
public class DefaultAuthorizationClient implements AuthorizationClient {

  private final ACLStore aclStore;

  /**
   * @param aclStore used to set and get {@link co.cask.common.authorization.ACLEntry}s
   */
  @Inject
  public DefaultAuthorizationClient(ACLStore aclStore) {
    this.aclStore = aclStore;
  }

  @Override
  public void authorize(Iterable<ObjectId> objects, Iterable<SubjectId> subjects,
                        Iterable<Permission> requiredPermissions) throws UnauthorizedException {
    if (!isAuthorized(objects, subjects, requiredPermissions)) {
      throw new UnauthorizedException(objects, subjects, requiredPermissions);
    }
  }

  @Override
  public void authorize(ObjectId object, Iterable<SubjectId> subjects,
                        Iterable<Permission> requiredPermissions) throws UnauthorizedException {
    if (!isAuthorized(object, subjects, requiredPermissions)) {
      throw new UnauthorizedException(Collections.singleton(object), subjects, requiredPermissions);
    }
  }

  @Override
  public boolean isAuthorized(Iterable<ObjectId> objects, Iterable<SubjectId> subjects,
                              Iterable<Permission> requiredPermissions) {

    Set<Permission> remainingRequiredPermission = Sets.newHashSet(requiredPermissions);
    for (ObjectId object : objects) {
      try {
        // TODO: consider doing only a single aclStore call
        List<ACLStore.Query> queries = generateQueries(object, subjects, remainingRequiredPermission);
        Set<ACLEntry> aclEntries = aclStore.search(queries);
        for (ACLEntry aclEntry : aclEntries) {
          remainingRequiredPermission.remove(aclEntry.getPermission());
        }

        if (remainingRequiredPermission.isEmpty()) {
          // Early exit if all permissions are fulfilled
          return true;
        }
      } catch (Exception e) {
        // Ignore since we assume unauthorized if there's an issue getting the ACLs
      }
    }

    return false;
  }

  private List<ACLStore.Query> generateQueries(ObjectId object, Iterable<SubjectId> subjects,
                                               Iterable<Permission> permissions) {
    List<ACLStore.Query> queries = Lists.newArrayList();
    for (SubjectId subjectId : subjects) {
      for (Permission permission : permissions) {
        queries.add(new ACLStore.Query(object, subjectId, permission));
      }
    }
    return queries;
  }

  @Override
  public boolean isAuthorized(ObjectId object, Iterable<SubjectId> subjects,
                              Iterable<Permission> requiredPermissions) {

    Set<Permission> remainingRequiredPermission = Sets.newHashSet(requiredPermissions);
    try {
      List<ACLStore.Query> query = generateQueries(object, subjects, remainingRequiredPermission);
      Set<ACLEntry> aclEntries = aclStore.search(query);
      for (ACLEntry aclEntry : aclEntries) {
        remainingRequiredPermission.remove(aclEntry.getPermission());
      }

      if (remainingRequiredPermission.isEmpty()) {
        // Early exit if all permissions are fulfilled
        return true;
      }
    } catch (Exception e) {
      // Ignore since we assume unauthorized if there's an issue getting the ACLs
    }

    return false;
  }

  @Override
  public <T extends IdentifiableObject> Iterable<T> filter(Iterable<T> objects,
                                                           final Iterable<SubjectId> subjects,
                                                           final Iterable<Permission> requiredPermissions) {
    // TODO: use only a single aclStore call
    return Iterables.filter(objects, new Predicate<IdentifiableObject>() {
      @Override
      public boolean apply(@Nullable IdentifiableObject input) {
        return input != null && isAuthorized(input.getObjectId(), subjects, requiredPermissions);
      }
    });
  }
}
