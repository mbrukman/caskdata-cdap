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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class describes how a dataset is partitioned, by means of the fields of a partition key and their types.
 */
public class Partitioning {

  /**
   * Describes the type of a partitioning field.
   */
  public enum FieldType {
    STRING {
      public String parse(String value) {
        return value;
      }
    },
    LONG {
      public Long parse(String value) {
        return Long.parseLong(value);
      }
    },
    INT {
      public Integer parse(String value) {
        return Integer.parseInt(value);
      }
    };

    public abstract Comparable parse(String value);
  }

  private final LinkedHashMap<String, FieldType> fields;

  /**
   * Private constructor to force the use of the builder.
   */
  private Partitioning(LinkedHashMap<String, FieldType> fields) {
    this.fields = fields;
  }

  /**
   * @return the type of a field, or null if that field is not declared for the partitioning
   */
  public FieldType getFieldType(String fieldName) {
    return fields.get(fieldName);
  }

  /**
   * @return all fields and their types
   */
  public Map<String, FieldType> getFields() {
    return ImmutableMap.copyOf(fields);
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

    private final LinkedHashMap<String, FieldType> fields = Maps.newLinkedHashMap();

    private Builder() { }

    /**
     * Add a field with a given name and type.
     * @param name the field name
     * @param type the type of the field
     */
    public void addField(String name, FieldType type) {
      fields.put(name, type);
    }

    /**
     * Add field of type STRING.
     * @param name the field name
     */
    public void addStringField(String name) {
      addField(name, FieldType.STRING);
    }

    /**
     * Add field of type INT.
     * @param name the field name
     */
    public void addIntField(String name) {
      addField(name, FieldType.INT);
    }

    /**
     * Add field of type LONG.
     * @param name the field name
     */
    public void addLongField(String name) {
      addField(name, FieldType.LONG);
    }

    /**
     * Create the partitioning.
     */
    public Partitioning build() {
      return new Partitioning(fields);
    }
  }

}
