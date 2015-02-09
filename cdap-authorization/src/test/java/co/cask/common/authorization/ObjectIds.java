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

/**
 * Various helper functions to construct {@link co.cask.common.authorization.ObjectId}s.
 */
public class ObjectIds {

  public static final String NAMESPACE = "namespace";
  public static final String APPLICATION = "app";

  public static ObjectId namespace(String id) {
    return new ObjectId(NAMESPACE, id);
  }

  public static ObjectId application(String namespaceId, String id) {
    return new ObjectId(new ObjectId(NAMESPACE, namespaceId), APPLICATION, id);
  }
}
