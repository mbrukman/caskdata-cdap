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

package co.cask.cdap.stream.store;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the {@link StreamMetaStore} implementations.
 */
public abstract class StreamMetaStoreTestBase {

  protected abstract StreamMetaStore getStreamMetaStore();

  protected abstract void createNamespace(String namespaceId);

  protected abstract void deleteNamespace(String namespaceId);

  @Before
  public void beforeTests() throws Exception {
    createNamespace("foo");
    createNamespace("foo1");
    createNamespace("foo2");
  }

  @After
  public void afterTests() throws Exception {
    deleteNamespace("foo");
    deleteNamespace("foo1");
    deleteNamespace("foo2");
  }

  @Test
  public void testStreamMetastore() throws Exception {
    StreamMetaStore streamMetaStore = getStreamMetaStore();

    streamMetaStore.addStream("foo", "bar");
    Assert.assertTrue(streamMetaStore.streamExists("foo", "bar"));
    Assert.assertFalse(streamMetaStore.streamExists("foofoo", "bar"));

    streamMetaStore.removeStream("foo", "bar");
    Assert.assertFalse(streamMetaStore.streamExists("foo", "bar"));

    streamMetaStore.addStream("foo1", "bar");
    streamMetaStore.addStream("foo2", "bar");
    Assert.assertEquals(ImmutableList.of(
      new StreamSpecification.Builder().setName("bar").create()), streamMetaStore.listStreams("foo1"));
    Assert.assertEquals(
      ImmutableMultimap.builder()
        .put(new NamespaceMeta.Builder().setId("foo1").build(),
             new StreamSpecification.Builder().setName("bar").create())
        .put(new NamespaceMeta.Builder().setId("foo2").build(),
             new StreamSpecification.Builder().setName("bar").create())
        .build(),
      streamMetaStore.listStreams());

    streamMetaStore.removeStream("foo2", "bar");
    Assert.assertFalse(streamMetaStore.streamExists("foo2", "bar"));
    Assert.assertEquals(
      ImmutableMultimap.builder()
        .put(new NamespaceMeta.Builder().setId("foo1").build(),
             new StreamSpecification.Builder().setName("bar").create())
        .build(),
      streamMetaStore.listStreams());

    streamMetaStore.removeStream("foo1", "bar");
  }
}
