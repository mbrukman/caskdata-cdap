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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.NamespaceHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Tests for {@link NamespaceHttpHandler}
 */
public class NamespaceHttpHandlerTest extends AppFabricTestBase {

  private static final Gson GSON = new Gson();
  private static final String EMPTY = "";
  private static final String ID_FIELD = "id";
  private static final String NAME_FIELD = "name";
  private static final String DESCRIPTION_FIELD = "description";
  private static final String ID = "test";
  private static final String NAME = "display test";
  private static final String DESCRIPTION = "test description";
  private static final NamespaceMeta METADATA_VALID = new NamespaceMeta.Builder().setName(NAME)
    .setDescription(DESCRIPTION).build();
  private static final NamespaceMeta METADATA_MISSING_FIELDS = new NamespaceMeta.Builder().build();
  private static final NamespaceMeta METADATA_EMPTY_FIELDS = new NamespaceMeta.Builder()
    .setName(EMPTY).setDescription(EMPTY).build();
  private static final String METADATA_INVALID_JSON = "invalid";
  private static final String INVALID_ID = "!nv@l*d/";

  private HttpResponse createNamespace(String id) throws Exception {
    return doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, id));
  }

  private HttpResponse createNamespace(NamespaceMeta metadata, String id) throws Exception {
    return createNamespace(GSON.toJson(metadata), id);
  }

  private HttpResponse createNamespace(String metadata, String id) throws Exception {
    return doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, id), metadata);
  }

  private HttpResponse listAllNamespaces() throws Exception {
    return doGet(String.format("%s/namespaces", Constants.Gateway.API_VERSION_3));
  }

  private HttpResponse getNamespace(String name) throws Exception {
    Preconditions.checkArgument(name != null, "namespace name cannot be null");
    return doGet(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, name));
  }

  private HttpResponse deleteNamespace(String name) throws Exception {
    return doDelete(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, name));
  }

  private void assertResponseCode(int expected, HttpResponse response) {
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

  private List<JsonObject> readListResponse(HttpResponse response) throws IOException {
    Type typeToken = new TypeToken<List<JsonObject>>() { }.getType();
    return readResponse(response, typeToken);
  }

  private JsonObject readGetResponse(HttpResponse response) throws IOException {
    Type typeToken = new TypeToken<JsonObject>() { }.getType();
    return readResponse(response, typeToken);
  }

  @Test
  public void testNamespacesValidFlows() throws Exception {
    // get initial namespace list
    HttpResponse response = listAllNamespaces();
    assertResponseCode(200, response);
    List<JsonObject> namespaces = readListResponse(response);
    int initialSize = namespaces.size();
    // create and verify
    response = createNamespace(METADATA_VALID, ID);
    assertResponseCode(200, response);
    response = listAllNamespaces();
    namespaces = readListResponse(response);
    Assert.assertEquals(initialSize + 1, namespaces.size());
    Assert.assertEquals(ID, namespaces.get(0).get(ID_FIELD).getAsString());
    Assert.assertEquals(NAME, namespaces.get(0).get(NAME_FIELD).getAsString());
    Assert.assertEquals(DESCRIPTION, namespaces.get(0).get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(ID);
    assertResponseCode(200, response);
    response = listAllNamespaces();
    namespaces = readListResponse(response);
    Assert.assertEquals(initialSize, namespaces.size());
  }

  @Test
  public void testCreateDuplicate() throws Exception {
    // prepare - create namespace
    HttpResponse response = createNamespace(METADATA_VALID, ID);
    assertResponseCode(200, response);
    response = getNamespace(ID);
    JsonObject namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(ID, namespace.get(ID_FIELD).getAsString());
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(DESCRIPTION, namespace.get(DESCRIPTION_FIELD).getAsString());

    // create again with the same name
    response = createNamespace(METADATA_EMPTY_FIELDS, ID);
    // create is idempotent, so response code is 200, but no updates should happen
    assertResponseCode(200, response);
    // check that no updates happened
    response = getNamespace(ID);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(ID, namespace.get(ID_FIELD).getAsString());
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(DESCRIPTION, namespace.get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(ID);
    assertResponseCode(200, response);
  }

  @Test
  public void testInvalidReservedId() throws Exception {
    HttpResponse response = createNamespace(METADATA_VALID, INVALID_ID);
    assertResponseCode(400, response);
    // 'default' and 'system' are reserved namespaces
    response = createNamespace(METADATA_VALID, Constants.DEFAULT_NAMESPACE);
    assertResponseCode(400, response);
    response = createNamespace(METADATA_VALID, Constants.SYSTEM_NAMESPACE);
    assertResponseCode(400, response);
    response = deleteNamespace(Constants.DEFAULT_NAMESPACE);
    assertResponseCode(403, response);
    response = deleteNamespace(Constants.SYSTEM_NAMESPACE);
    assertResponseCode(403, response);
  }

  @Test
  public void testCreateInvalidJson() throws Exception {
    // invalid json should return 400
    HttpResponse response = createNamespace(METADATA_INVALID_JSON, ID);
    assertResponseCode(400, response);
    // verify
    response = getNamespace(ID);
    assertResponseCode(404, response);
  }

  @Test
  public void testCreateMissingOrEmptyFields() throws Exception {
    // create with no metadata
    HttpResponse response = createNamespace(ID);
    assertResponseCode(200, response);
    // verify
    response = getNamespace(ID);
    JsonObject namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(ID, namespace.get(ID_FIELD).getAsString());
    Assert.assertEquals(ID, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(EMPTY, namespace.get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(ID);
    assertResponseCode(200, response);

    // create with missing fields
    response = createNamespace(METADATA_MISSING_FIELDS, ID);
    assertResponseCode(200, response);
    // verify
    response = getNamespace(ID);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(ID, namespace.get(ID_FIELD).getAsString());
    Assert.assertEquals(ID, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(EMPTY, namespace.get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(ID);
    assertResponseCode(200, response);

    // create with empty fields
    response = createNamespace(METADATA_EMPTY_FIELDS, ID);
    assertResponseCode(200, response);
    // verify
    response = getNamespace(ID);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(ID, namespace.get(ID_FIELD).getAsString());
    Assert.assertEquals(EMPTY, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(EMPTY, namespace.get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(ID);
    assertResponseCode(200, response);
  }

  @Test
  public void testDeleteMissingNamespace() throws Exception {
    // test deleting non-existent namespace
    HttpResponse response = deleteNamespace("doesnotexist");
    assertResponseCode(404, response);
  }
}
