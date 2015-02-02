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
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Set;

/**
 * Provides ways to verify, create, and list ACL entries.
 */
public class ACLManagerClient {

  private static final Gson GSON = new Gson();
  private final Supplier<URI> baseURISupplier;

  public ACLManagerClient(Supplier<URI> baseURISupplier) {
    this.baseURISupplier = baseURISupplier;
  }

  public String appendCondition(String path, ACLStore.Query query) {
    List<String> arguments = Lists.newArrayList();

    int i = 0;
    for (ACLStore.Condition condition : query.getConditions()) {
      if (condition.getSubjectId().isPresent()) {
        arguments.add("subject[" + i + "]=" + condition.getSubjectId().get().getRep());
      }
      if (condition.getObjectId().isPresent()) {
        arguments.add("object[" + i + "]=" + condition.getObjectId().get().getRep());
      }
      if (condition.getPermission().isPresent()) {
        arguments.add("permission[" + i + "]=" + condition.getPermission().get().getName());
      }
      i++;
    }

    if (!arguments.isEmpty()) {
      return path + "?" + Joiner.on("&").join(arguments);
    }

    return path;
  }

  public Set<ACLEntry> getGlobalACLs(ACLStore.Query query) throws IOException {
    String path = appendCondition("/v1/acls/global", query);
    HttpRequest request = HttpRequest.get(resolveURL(path)).build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<Set<ACLEntry>>() { }).getResponseObject();
  }

  public Set<ACLEntry> getACLs(String namespaceId, ACLStore.Query query) throws IOException {
    String path = appendCondition("/v1/acls/namespace/" + namespaceId, query);
    HttpRequest request = HttpRequest.get(resolveURL(path)).build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<Set<ACLEntry>>() { }).getResponseObject();
  }

  public void deleteGlobalACLs(ACLStore.Query query) throws IOException {
    String path = appendCondition("/v1/acls/global", query);
    HttpRequest request = HttpRequest.delete(resolveURL(path)).build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }
  }

  public void deleteACLs(String namespaceId, ACLStore.Query query) throws IOException {
    String path = appendCondition("/v1/acls/namespace/" + namespaceId, query);
    HttpRequest request = HttpRequest.delete(resolveURL(path)).build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }
  }

  /**
   * Creates an {@link ACLEntry} in a namespace for an object, subject, and a permission.
   * This allows the subject to access the object for the specified permission.
   *
   * <p>
   * For example, if object is "secretFile", subject is "Bob", and permission is "WRITE", then "Bob"
   * would be allowed to write to the "secretFile", assuming that what is doing the writing is protecting
   * the "secretFile" via a call to one of the {@code verifyAuthorized()} or {@code isAuthorized()} calls.
   * </p>
   *
   * @param entry the {@link ACLEntry} to create
   * @throws java.io.IOException if an error occurred when contacting the authorization service
   */
  public void createACL(String namespaceId, ACLEntry entry) throws IOException {
    HttpRequest request = HttpRequest.post(resolveURL("/v1/acls/namespace/" + namespaceId))
      .withBody(GSON.toJson(entry)).build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK &&
      response.getResponseCode() != HttpURLConnection.HTTP_NOT_MODIFIED) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }
  }

  /**
   * Creates an {@link ACLEntry} for the global namespace, subject, and a permission.
   * This allows the subject to access the object for the specified permission.
   *
   * <p>
   * For example, if object is "secretFile", subject is "Bob", and permission is "WRITE", then "Bob"
   * would be allowed to write to the "secretFile", assuming that what is doing the writing is protecting
   * the "secretFile" via a call to one of the {@code verifyAuthorized()} or {@code isAuthorized()} calls.
   * </p>
   *
   * @param entry the {@link ACLEntry} to create
   * @throws java.io.IOException if an error occurred when contacting the authorization service
   */
  public void createGlobalACL(ACLEntry entry) throws IOException {
    HttpRequest request = HttpRequest.post(resolveURL("/v1/acls/global"))
      .withBody(GSON.toJson(entry)).build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK &&
      response.getResponseCode() != HttpURLConnection.HTTP_NOT_MODIFIED) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }
  }

  protected URL resolveURL(String path) throws MalformedURLException {
    return baseURISupplier.get().resolve(path).toURL();
  }
}
