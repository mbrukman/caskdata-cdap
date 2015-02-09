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

import co.cask.common.authorization.client.AuthorizationClient;
import co.cask.common.authorization.client.DefaultAuthorizationClient;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Test for {@link ACLStore}.
 */
public class ACLStoreTest {

  private AuthorizationClient authorizationClient;
  private ACLStore aclStore;

  @Before
  public void setUp() {
    this.aclStore = new InMemoryACLStore();
    this.authorizationClient = new DefaultAuthorizationClient(aclStore);
  }

  @Test
  public void testAuthorized() throws Exception {
    SubjectId currentUser = SubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = ObjectIds.application(namespaceId, "someApp");
    Permission permission = Permission.WRITE;

    aclStore.write(new ACLEntry(objectId, currentUser, permission));
    authorizationClient.authorize(objectId, ImmutableSet.of(currentUser), ImmutableSet.of(permission));
  }

  @Test
  public void testUnauthorizedNoACLEntry() throws Exception {
    SubjectId currentUser = SubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = ObjectIds.application(namespaceId, "someApp");
    Permission permission = Permission.WRITE;

    try {
      authorizationClient.authorize(objectId, ImmutableSet.of(currentUser), ImmutableSet.of(permission));
      Assert.fail();
    } catch (UnauthorizedException e) {
      Assert.assertTrue(e.getMessage() + " should contain namespaceId: " + namespaceId,
                        e.getMessage().contains(namespaceId));
      Assert.assertTrue(e.getMessage() + " should contain objectId: " + objectId.toString(),
                        e.getMessage().contains(objectId.toString()));
      Assert.assertTrue(e.getMessage() + " should contain permission: " + permission.toString(),
                        e.getMessage().contains(permission.toString()));
    }
  }

  @Test
  public void testUnauthorizedWrongPermission() throws Exception {
    SubjectId currentUser = SubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = ObjectIds.application(namespaceId, "someApp");
    Permission permission = Permission.WRITE;

    Permission wrongPermission = Permission.ADMIN;
    Assert.assertNotEquals(wrongPermission, permission);
    aclStore.write(new ACLEntry(objectId, currentUser, wrongPermission));

    try {
      authorizationClient.authorize(objectId, ImmutableSet.of(currentUser), ImmutableSet.of(permission));
      Assert.fail();
    } catch (UnauthorizedException e) {
      Assert.assertTrue(e.getMessage() + " should contain namespaceId: " + namespaceId,
                        e.getMessage().contains(namespaceId));
      Assert.assertTrue(e.getMessage() + " should contain objectId: " + objectId.toString(),
                        e.getMessage().contains(objectId.toString()));
      Assert.assertTrue(e.getMessage() + " should contain permission: " + permission.toString(),
                        e.getMessage().contains(permission.toString()));
    }
  }

  @Test
  public void testUnauthorizedWrongUser() throws Exception {
    SubjectId currentUser = SubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = ObjectIds.application(namespaceId, "someApp");
    Permission permission = Permission.WRITE;

    SubjectId wrongUser = SubjectIds.user("wrong");
    Assert.assertNotEquals(wrongUser, currentUser);
    aclStore.write(new ACLEntry(objectId, wrongUser, permission));

    try {
      authorizationClient.authorize(objectId, ImmutableSet.of(currentUser), ImmutableSet.of(permission));
      Assert.fail();
    } catch (UnauthorizedException e) {
      Assert.assertTrue(e.getMessage() + " should contain namespaceId: " + namespaceId,
                        e.getMessage().contains(namespaceId));
      Assert.assertTrue(e.getMessage() + " should contain objectId: " + objectId.toString(),
                        e.getMessage().contains(objectId.toString()));
      Assert.assertTrue(e.getMessage() + " should contain permission: " + permission.toString(),
                        e.getMessage().contains(permission.toString()));
    }
  }

  @Test
  public void testUnauthorizedWrongObject() throws Exception {
    SubjectId currentUser = SubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = ObjectIds.application(namespaceId, "someApp");
    Permission permission = Permission.WRITE;

    ObjectId wrongObject = ObjectIds.application(namespaceId, "wrong");
    Assert.assertNotEquals(wrongObject, objectId);
    aclStore.write(new ACLEntry(wrongObject, currentUser, permission));

    try {
      authorizationClient.authorize(objectId, ImmutableSet.of(currentUser), ImmutableSet.of(permission));
      Assert.fail();
    } catch (UnauthorizedException e) {
      Assert.assertTrue(e.getMessage() + " should contain namespaceId: " + namespaceId,
                        e.getMessage().contains(namespaceId));
      Assert.assertTrue(e.getMessage() + " should contain objectId: " + objectId.toString(),
                        e.getMessage().contains(objectId.toString()));
      Assert.assertTrue(e.getMessage() + " should contain permission: " + permission.toString(),
                        e.getMessage().contains(permission.toString()));
    }
  }

  @Test
  public void testUnauthorizedWrongNamespace() throws Exception {
    SubjectId currentUser = SubjectIds.user("bob");
    String namespaceId = "someNamespace";
    String otherNamespaceId = "otherNamespace";
    ObjectId objectId = ObjectIds.application(namespaceId, "someApp");
    ObjectId objectIdInOtherNamespace = ObjectIds.application(otherNamespaceId, "someApp");
    Permission permission = Permission.WRITE;

    aclStore.write(new ACLEntry(objectIdInOtherNamespace, currentUser, permission));

    try {
      authorizationClient.authorize(objectId, ImmutableSet.of(currentUser), ImmutableSet.of(permission));
      Assert.fail();
    } catch (UnauthorizedException e) {
      Assert.assertTrue(e.getMessage() + " should contain namespaceId: " + namespaceId,
                        e.getMessage().contains(namespaceId));
      Assert.assertTrue(e.getMessage() + " should contain objectId: " + objectId.toString(),
                        e.getMessage().contains(objectId.toString()));
      Assert.assertTrue(e.getMessage() + " should contain permission: " + permission.toString(),
                        e.getMessage().contains(permission.toString()));
    }
  }

  @Test
  public void testFilter() throws Exception {
    SubjectId currentUser = SubjectIds.user("bob");
    String namespaceId = "someNamespace";
    Permission permission = Permission.WRITE;

    TestApp someApp = new TestApp(namespaceId, "someApp");
    TestApp secretApp = new TestApp(namespaceId, "secretApp");
    List<TestApp> objects = ImmutableList.of(someApp, secretApp);

    aclStore.write(new ACLEntry(someApp.getObjectId(), currentUser, permission));
    List<TestApp> filtered = ImmutableList.copyOf(
      authorizationClient.filter(objects, ImmutableSet.of(currentUser), ImmutableSet.of(permission)));
    Assert.assertEquals(1, filtered.size());
    Assert.assertEquals(someApp, filtered.get(0));
  }

  /**
   *
   */
  private static final class TestApp implements IdentifiableObject {

    private final String id;
    private final String namespaceId;

    private TestApp(String namespaceId, String id) {
      this.namespaceId = namespaceId;
      this.id = id;
    }

    @Override
    public ObjectId getObjectId() {
      return ObjectIds.application(namespaceId, id);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id, namespaceId);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final TestApp other = (TestApp) obj;
      return Objects.equal(this.id, other.id) && Objects.equal(this.namespaceId, other.namespaceId);
    }
  }
}
