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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.authorization.ObjectIds;
import co.cask.cdap.authorization.SubjectIds;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.common.authorization.ACLEntry;
import co.cask.common.authorization.ACLStore;
import co.cask.common.authorization.Permission;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

/**
 * Test for {@link ACLStoreTableDataset}.
 */
public class ACLStoreTableDatasetTest extends AbstractDatasetTest {

  private static final ACLEntry SIMPLE_ACL = new ACLEntry(
    ObjectIds.application("someNamespace", "someApp"),
    SubjectIds.user("bob"),
    Permission.WRITE
  );

  private ACLStoreTable aclStore;

  @Before
  public void setUp() throws Exception {
    addModule(ACLStoreTableModule.class.getName(), new ACLStoreTableModule());
    createInstance(ACLStoreTable.class.getName(), "testACLStoreTable", DatasetProperties.EMPTY);
    this.aclStore = getInstance("testACLStoreTable");
  }

  @After
  public void tearDown() throws Exception {
    deleteInstance("testACLStoreTable");
    deleteModule(ACLStoreTableModule.class.getName());
  }

  @Test
  public void testWriteAndDelete() throws Exception {
    Assert.assertFalse(aclStore.exists(SIMPLE_ACL));
    aclStore.write(SIMPLE_ACL);
    Assert.assertTrue(aclStore.exists(SIMPLE_ACL));
    aclStore.delete(SIMPLE_ACL);
    Assert.assertFalse(aclStore.exists(SIMPLE_ACL));
  }

  @Test
  public void testSearchAndDeleteBySimpleQuery() throws Exception {
    testSearchAndDelete(ImmutableList.of(SIMPLE_ACL),
                        ImmutableList.of(new ACLStore.Query(SIMPLE_ACL)));
  }

  @Test
  public void testSearchAndDeleteByObject() throws Exception {
    testSearchAndDelete(ImmutableList.of(SIMPLE_ACL),
                        ImmutableList.of(new ACLStore.Query(SIMPLE_ACL.getObject(), null, null)));
  }

  @Test
  public void testSearchAndDeleteBySubject() throws Exception {
    testSearchAndDelete(ImmutableList.of(SIMPLE_ACL),
                        ImmutableList.of(new ACLStore.Query(null, SIMPLE_ACL.getSubject(), null)));
  }

  @Test
  public void testSearchAndDeleteByPermission() throws Exception {
    testSearchAndDelete(ImmutableList.of(SIMPLE_ACL),
                        ImmutableList.of(new ACLStore.Query(null, null, SIMPLE_ACL.getPermission())));
  }
  
  @Test
  public void testMultiSearchAndDelete() throws Exception {
    ACLEntry sameObjectAndSubject = new ACLEntry(SIMPLE_ACL);
    sameObjectAndSubject.setPermission(Permission.EXECUTE);
    Assert.assertNotEquals(sameObjectAndSubject.getPermission(), SIMPLE_ACL.getPermission());

    testSearchAndDelete(ImmutableList.of(SIMPLE_ACL, sameObjectAndSubject),
                        ImmutableList.of(new ACLStore.Query(SIMPLE_ACL.getObject(), SIMPLE_ACL.getSubject(), null)));
  }

  protected void testSearchAndDelete(List<ACLEntry> entries, Iterable<ACLStore.Query> query) throws Exception {
    // create and check all entries exist
    for (ACLEntry entry : entries) {
      aclStore.write(entry);
      Assert.assertTrue(aclStore.exists(entry));
    }

    // search and delete all entries
    Set<ACLEntry> searchResults = aclStore.search(query);
    for (ACLEntry entry : entries) {
      aclStore.write(entry);
      Assert.assertTrue(searchResults.contains(entry));
    }
    Assert.assertEquals(entries.size(), searchResults.size());
    aclStore.delete(query);

    // ensure all entries deleted
    for (ACLEntry entry : entries) {
      Assert.assertFalse(aclStore.exists(entry));
    }
  }
}
