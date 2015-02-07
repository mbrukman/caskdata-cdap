/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.proto;

import com.google.common.base.CharMatcher;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Contains collection of classes representing different types of Ids.
 */
public final class Id  {

  private static boolean isId(String name) {
    return CharMatcher.inRange('A', 'Z')
      .or(CharMatcher.inRange('a', 'z'))
      .or(CharMatcher.is('-'))
      .or(CharMatcher.is('_'))
      .or(CharMatcher.inRange('0', '9')).matchesAllOf(name);
  }

  /**
   * Represents ID of a namespace.
   */
  public static final class Namespace {
    private final String id;

    public Namespace(String id) {
      Preconditions.checkNotNull(id, "Namespace cannot be null.");
      this.id = id;
    }

    public String getId() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      return id.equals(((Namespace) o).id);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id);
    }

    public static Namespace from(String namespace) {
      return new Namespace(namespace);
    }
  }

  /**
   * Program Id identifies a given application.
   * Application is global unique if used within context of namespace.
   */
  public static final class Application {
    private final Namespace namespace;
    private final String applicationId;

    public Application(final Namespace namespace, final String applicationId) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null.");
      Preconditions.checkNotNull(applicationId, "Application cannot be null.");
      this.namespace = namespace;
      this.applicationId = applicationId;
    }

    public Namespace getNamespace() {
      return namespace;
    }

    public String getNamespaceId() {
      return namespace.getId();
    }

    public String getId() {
      return applicationId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Application that = (Application) o;
      return namespace.equals(that.namespace) && applicationId.equals(that.applicationId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(namespace, applicationId);
    }

    public static Application from(Namespace id, String application) {
      return new Application(id, application);
    }

    public static Application from(String namespaceId, String applicationId) {
      return new Application(Namespace.from(namespaceId), applicationId);
    }
  }

  /**
   * Program Id identifies a given program.
   * Program is global unique if used within context of namespace and application.
   */
  public static class Program {
    private final Application application;
    private final String id;

    public Program(Application application, final String id) {
      Preconditions.checkNotNull(application, "Application cannot be null.");
      Preconditions.checkNotNull(id, "Id cannot be null.");
      this.application = application;
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public String getApplicationId() {
      return application.getId();
    }

    public String getNamespaceId() {
      return application.getNamespaceId();
    }

    public Application getApplication() {
      return application;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Program program = (Program) o;
      return application.equals(program.application) && id.equals(program.id);
    }

    @Override
    public int hashCode() {
      int result = application.hashCode();
      result = 31 * result + id.hashCode();
      return result;
    }

    public static Program from(Application appId, String pgmId) {
      return new Program(appId, pgmId);
    }

    public static Program from(String namespaceId, String appId, String pgmId) {
      return new Program(new Application(new Namespace(namespaceId), appId), pgmId);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("ProgramId(");

      sb.append("namespaceId:");
      if (this.application.getNamespaceId() == null) {
        sb.append("null");
      } else {
        sb.append(this.application.getNamespaceId());
      }
      sb.append(", applicationId:");
      if (this.application.getId() == null) {
        sb.append("null");
      } else {
        sb.append(this.application.getId());
      }
      sb.append(", runnableId:");
      if (this.id == null) {
        sb.append("null");
      } else {
        sb.append(this.id);
      }
      sb.append(")");
      return sb.toString();
    }
  }

  /**
   * Represents ID of a Notification feed.
   */
  public static class NotificationFeed {

    private final Namespace namespace;
    private final String category;
    private final String name;

    private final String description;

    /**
     * {@link NotificationFeed} object from an id in the form of "namespace.category.name".
     *
     * @param id id of the notification feed to build
     * @return a {@link NotificationFeed} object which id is the same as {@code id}
     * @throws IllegalArgumentException when the id doesn't match a valid feed id
     */
    public static NotificationFeed fromId(String id) {
      String[] idParts = id.split("\\.");
      if (idParts.length != 3) {
        throw new IllegalArgumentException(String.format("Id %s is not a valid feed id.", id));
      }
      return new NotificationFeed(idParts[0], idParts[1], idParts[2], "");
    }

    private NotificationFeed(String namespace, String category, String name, String description) {
      Preconditions.checkArgument(namespace != null && !namespace.isEmpty(),
                                  "Namespace value cannot be null or empty.");
      Preconditions.checkArgument(category != null && !category.isEmpty(), "Category value cannot be null or empty.");
      Preconditions.checkArgument(name != null && !name.isEmpty(), "Name value cannot be null or empty.");
      Preconditions.checkArgument(isId(namespace) && isId(category) && isId(name),
                                  "Namespace, category or name has a wrong format.");

      this.namespace = Namespace.from(namespace);
      this.category = category;
      this.name = name;
      this.description = description;
    }

    public String getCategory() {
      return category;
    }

    public String getId() {
      return String.format("%s.%s.%s", namespace.getId(), category, name);
    }

    public String getNamespaceId() {
      return namespace.getId();
    }

    public String getName() {
      return name;
    }

    public String getDescription() {
      return description;
    }

    /**
     * Builder used to build {@link NotificationFeed}.
     */
    public static final class Builder {
      private String category;
      private String name;
      private String namespaceId;
      private String description;

      public Builder() {
        // No-op
      }

      public Builder(NotificationFeed feed) {
        this.namespaceId = feed.getNamespaceId();
        this.category = feed.getCategory();
        this.name = feed.getName();
        this.description = feed.getDescription();
      }

      public Builder setName(final String name) {
        this.name = name;
        return this;
      }

      public Builder setNamespaceId(final String namespace) {
        this.namespaceId = namespace;
        return this;
      }

      public Builder setDescription(final String description) {
        this.description = description;
        return this;
      }

      public Builder setCategory(final String category) {
        this.category = category;
        return this;
      }

      /**
       * @return a {@link NotificationFeed} object containing all the fields set in the builder.
       * @throws IllegalArgumentException if the namespaceId, category or name is invalid.
       */
      public NotificationFeed build() {
        return new NotificationFeed(namespaceId, category, name, description);
      }
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("namespace", namespace)
        .add("category", category)
        .add("name", name)
        .add("description", description)
        .toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      NotificationFeed that = (NotificationFeed) o;
      return Objects.equal(this.namespace, that.namespace)
        && Objects.equal(this.category, that.category)
        && Objects.equal(this.name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(namespace, category, name);
    }
  }
}
