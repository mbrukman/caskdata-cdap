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

package co.cask.cdap.api.dataset.lib;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Types and methods to specify partitioning keys for datasets.
 */
public class PartitionKey {

  private final Map<String, Comparable> fields;

  /**
   * Private constructor to force use of the builder.
   */
  private PartitionKey(Map<String, Comparable> fields) {
    Preconditions.checkNotNull(fields, "Partition fields cannot be null.");
    Preconditions.checkArgument(!fields.isEmpty(), "Partition fields cannot be empty.");
    this.fields = fields;
  }

  /**
   * @return all field names and their values in a map.
   */
  public Map<String, ? extends Comparable> getFields() {
    return fields;
  }

  /**
   * @return the value of a field
   */
  public Comparable getField(String fieldName) {
    return fields.get(fieldName);
  }

  @Override
  public String toString() {
    return fields.toString();
  }

  @Override
  public boolean equals(Object other) {
    return this == other ||
      (other != null && getClass() == other.getClass()
        && getFields().equals(((PartitionKey) other).getFields())); // fields is never null
  }

  @Override
  public int hashCode() {
    return fields.hashCode(); // fields is never null
  }

  /**
   * @return a builder for a partitioning
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * A builder for partitioning objects.
   */
  public static class Builder {

    private final LinkedHashMap<String, Comparable> fields = Maps.newLinkedHashMap();

    private Builder() { }

    /**
     * Add a field with a given name and value.
     * @param name the field name
     * @param value the value of the field
     */
    public <T extends Comparable<T>> Builder addField(String name, T value) {
      fields.put(name, value);
      return this;
    }

    /**
     * Add field of type STRING.
     * @param name the field name
     * @param value the value of the field
     */
    public Builder addStringField(String name, String value) {
      return addField(name, value);
    }

    /**
     * Add field of type INT.
     * @param name the field name
     * @param value the value of the field
     */
    public Builder addIntField(String name, int value) {
      return addField(name, value);
    }

    /**
     * Add field of type LONG.
     * @param name the field name
     * @param value the value of the field
     */
    public Builder addLongField(String name, long value) {
      return addField(name, value);
    }

    /**
     * Create the partition key.
     */
    public PartitionKey build() {
      return new PartitionKey(fields);
    }
  }

}
