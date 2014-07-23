/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.reactor.client;

import com.continuuity.common.http.HttpMethod;
import com.continuuity.common.http.HttpRequest;
import com.continuuity.common.http.HttpResponse;
import com.continuuity.common.http.ObjectResponse;
import com.continuuity.reactor.client.config.ReactorClientConfig;
import com.continuuity.reactor.client.exception.ApplicationNotFoundException;
import com.continuuity.reactor.client.util.RestClient;
import com.continuuity.reactor.metadata.ApplicationRecord;
import com.continuuity.reactor.metadata.ProgramRecord;
import com.continuuity.reactor.metadata.ProgramType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

/**
 * Provides ways to interact with Reactor applications.
 */
public class ReactorAppClient {

  private final RestClient restClient;
  private final ReactorClientConfig config;

  @Inject
  public ReactorAppClient(ReactorClientConfig config) {
    this.config = config;
    this.restClient = RestClient.create(config);
  }

  /**
   * Lists all applications currently deployed.
   *
   * @return list of {@link ApplicationRecord}s.
   * @throws IOException
   */
  public List<ApplicationRecord> list() throws IOException {
    HttpResponse response = restClient.execute(HttpMethod.GET, config.resolveURL("apps"));
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<ApplicationRecord>>() { }).getResponseObject();
  }

  /**
   * Deletes an application.
   *
   * @param appId ID of the application to delete
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   */
  public void delete(String appId) throws ApplicationNotFoundException, IOException {
    HttpResponse response = restClient.execute(HttpMethod.DELETE, config.resolveURL("apps/" + appId),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(appId);
    }
  }

  /**
   * Deploys an application.
   *
   * @param jarFile jar file of the application to deploy
   * @throws IOException if a network error occurred
   */
  public void deploy(File jarFile) throws IOException {
    URL url = config.resolveURL("apps");
    Map<String, String> headers = ImmutableMap.of("X-Archive-Name", jarFile.getName());

    HttpRequest request = HttpRequest.post(url).addHeaders(headers).withBody(jarFile).build();
    restClient.upload(request);
  }

  /**
   * Promotes an application to another environment.
   *
   * @param appId ID of the application to promote
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   */
  public void promote(String appId) throws ApplicationNotFoundException, IOException {
    URL url = config.resolveURL("apps/" + appId);

    HttpResponse response = restClient.execute(HttpMethod.POST, url, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(appId);
    }
  }

  /**
   * Lists all programs of some type.
   *
   * @param programType type of the programs to list
   * @return list of {@link ProgramRecord}s
   * @throws IOException if a network error occurred
   */
  public List<ProgramRecord> listAllPrograms(ProgramType programType) throws IOException {

    URL url = config.resolveURL(programType.getCategoryName());
    HttpRequest request = HttpRequest.get(url).build();

    ObjectResponse<List<ProgramRecord>> response = ObjectResponse.fromJsonBody(
      restClient.execute(request), new TypeToken<List<ProgramRecord>>() { });

    return response.getResponseObject();
  }

  /**
   * Lists all programs.
   *
   * @return list of {@link ProgramRecord}s
   * @throws IOException if a network error occurred
   */
  public Map<ProgramType, List<ProgramRecord>> listAllPrograms() throws IOException {

    ImmutableMap.Builder<ProgramType, List<ProgramRecord>> allPrograms = ImmutableMap.builder();
    for (ProgramType programType : ProgramType.values()) {
      if (programType.isListable()) {
        List<ProgramRecord> programRecords = Lists.newArrayList();
        programRecords.addAll(listAllPrograms(programType));
        allPrograms.put(programType, programRecords);
      }
    }
    return allPrograms.build();
  }
  

  /**
   * Lists programs of some type beloing to an application.
   *
   * @param appId ID of the application
   * @param programType type of the programs to list
   * @return list of {@link ProgramRecord}s
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   */
  public List<ProgramRecord> listPrograms(String appId, ProgramType programType)
    throws ApplicationNotFoundException, IOException {

    URL url = config.resolveURL(String.format("apps/%s/%s", appId, programType.getCategoryName()));
    HttpRequest request = HttpRequest.get(url).build();

    ObjectResponse<List<ProgramRecord>> response = ObjectResponse.fromJsonBody(
      restClient.execute(request, HttpURLConnection.HTTP_NOT_FOUND), new TypeToken<List<ProgramRecord>>() { });

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(appId);
    }

    return response.getResponseObject();
  }

  /**
   * Lists programs belonging to an application.
   *
   * @param appId ID of the application
   * @return Map of {@link ProgramType} to list of {@link ProgramRecord}s
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   */
  public Map<ProgramType, List<ProgramRecord>> listPrograms(String appId)
    throws ApplicationNotFoundException, IOException {

    ImmutableMap.Builder<ProgramType, List<ProgramRecord>> allPrograms = ImmutableMap.builder();
    for (ProgramType programType : ProgramType.values()) {
      if (programType.isListable()) {
        List<ProgramRecord> programRecords = Lists.newArrayList();
        programRecords.addAll(listPrograms(appId, programType));
        allPrograms.put(programType, programRecords);
      }
    }
    return allPrograms.build();
  }
}