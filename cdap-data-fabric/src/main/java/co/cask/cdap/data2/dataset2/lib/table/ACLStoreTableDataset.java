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

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.IndexedObjectStore;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.common.authorization.ACLEntry;
import com.google.gson.Gson;

import java.util.Set;

/**
 * Dataset that manages a table of ACLs.
 */
public class ACLStoreTableDataset extends AbstractDataset implements ACLStoreTable {

  private static final Gson GSON = new Gson();

  private final IndexedObjectStore<ACLEntry> store;

  public ACLStoreTableDataset(DatasetSpecification spec,
                              @EmbeddedDataset("acls") IndexedObjectStore<ACLEntry> store) {
    super(spec.getName(), store);
    this.store = store;
  }

  @Override
  public void write(ACLEntry entry) throws Exception {

  }

  @Override
  public void exists(ACLEntry entry) throws Exception {

  }

  @Override
  public void delete(ACLEntry entry) throws Exception {

  }

  @Override
  public Set<ACLEntry> search(Query query) throws Exception {
    return null;
  }

  @Override
  public void delete(Query query) throws Exception {

  }
}
