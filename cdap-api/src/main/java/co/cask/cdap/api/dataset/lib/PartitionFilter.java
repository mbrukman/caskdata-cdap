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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Describes a filter over partition keys.
 */
public class PartitionFilter {

  private final Map<String, Condition<? extends Comparable>> conditions;

  // we only allow creating an filter through the builder.
  private PartitionFilter(Map<String, Condition<? extends Comparable>> conditions) {
    this.conditions = conditions;
  }

  /**
   * This should be used for inspection or debugging only.
   * To match this filter, use the {@link #match} method.
   *
   * @return the individual conditions of this filter.
   */
  public Map<String, Condition<? extends Comparable>> getConditions() {
    return ImmutableMap.copyOf(conditions);
  }

  /**
   * @return the condition for a particular field.
   */
  public Condition<? extends Comparable> getCondition(String fieldName) {
    return conditions.get(fieldName);
  }

  /**
   * Match this filter against a partition key. The key matches iff it matches all conditions.
   *
   * @throws java.lang.IllegalArgumentException if one of the field types in the partition key are incompatible
   */
  public boolean match(PartitionKey partitionKey) {
    for (Map.Entry<String, Condition<? extends Comparable>> condition : getConditions().entrySet())  {
      Comparable value = partitionKey.getField(condition.getKey());
      if (value == null || !condition.getValue().match(value)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return conditions.values().toString();
  }

  /**
   * Use this to create PartitionFilters.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * A builder for partition filters.
   */
  public static class Builder {
    private final Map<String, Condition<? extends Comparable>> map = Maps.newLinkedHashMap();

    /**
     * Add a condition for a given field name with an inclusive lower and an exclusive upper bound.
     * Either bound can be null, meaning unbounded in that direction. If both upper and lower bound are
     * null, then this condition has no effect and is not added to the filter.
     *
     * @param field The name of the partition field
     * @param lower the inclusive lower bound. If null, there is no lower bound.
     * @param upper the exclusive upper bound. If null, there is no upper bound.
     * @param <T> The type of the partition field
     *
     * @throws java.lang.IllegalArgumentException if the field name is null, empty, or already exists,
     *         or if both bounds are equal (meaning the condition cannot be satisfied).
     */
    public <T extends Comparable<T>> Builder addRangeCondition(String field,
                                                               @Nullable T lower,
                                                               @Nullable T upper) {
      Preconditions.checkArgument(field != null && !field.isEmpty(), "field name cannot be null or empty.");
      Preconditions.checkArgument(lower == null || !lower.equals(upper), "Unsatisfiable condition: " +
        "lower bound and upper bound have equal value: '" + lower + "'");
      if (map.containsKey(field)) {
        throw new IllegalArgumentException(String.format("Field '%s' already exists in partition filter.", field));
      }
      if (null == lower && null == upper) { // filter is pointless if there is no bound
        return this;
      }
      map.put(field, new Condition<T>(field, lower, upper));
      return this;
    }

    /**
     * Add a condition that matches by equality.
     *
     * @param field The name of the partition field
     * @param value The value that matching field values must have
     * @param <T> The type of the partition field
     *
     * @throws java.lang.IllegalArgumentException if the field name is null, empty, or already exists,
     *         or if the value is null.
     */
    public <T extends Comparable<T>> Builder addValueCondition(String field, T value) {
      Preconditions.checkArgument(field != null && !field.isEmpty(), "field name cannot be null or empty.");
      Preconditions.checkArgument(value != null, "condition value cannot be null.");
      if (map.containsKey(field)) {
        throw new IllegalArgumentException(String.format("Field '%s' already exists in partition filter.", field));
      }
      map.put(field, new Condition<T>(field, value, value));
      return this;
    }

    /**
     * Create the PartitionFilter.
     *
     * @throws java.lang.IllegalStateException if no fields have been added
     */
    public PartitionFilter build() {
      Preconditions.checkState(!map.isEmpty(), "Partition filter cannot be empty.");
      return new PartitionFilter(map);
    }
  }

  /**
   * Represents a condition on a partitioning field, by means of an inclusive lower bound and an exclusive upper bound.
   * As a special case, if upper and lower bound are identical (not just equal), then this represents a condition
   * of equality with that value.
   * @param <T> The type of the partitioning field.
   */
  public static class Condition<T extends Comparable<T>> {

    private String fieldName;
    private final T lower;
    private final T upper;

    private Condition(String fieldName, T lower, T upper) {
      Preconditions.checkArgument(lower != null || upper != null, "Either lower or upper-bound must be non-null.");
      this.fieldName = fieldName;
      this.lower = lower;
      this.upper = upper;
    }

    /**
     * @return the lower bound of this condition
     */
    public T getLower() {
      return lower;
    }

    /**
     * @return the upper bound of this condition
     */
    public T getUpper() {
      return upper;
    }

    /**
     * @return whether this condition matches a single value
     */
    public boolean isSingleValue() {
      return lower == upper;
    }

    /**
     * Match the condition against a given value. The value must be of the same type as the bounds of the condition.
     *
     * @throws java.lang.IllegalArgumentException if the value has an incompatible type
     */
    public <V extends Comparable> boolean match(V value) {
      try {
        // if lower and upper are identical, then this represents an equality condition.
        if (lower == upper) {
          return lower.equals(value);
        }
        @SuppressWarnings("unchecked")
        boolean matches =
          (lower == null || lower.compareTo((T) value) <= 0) &&
            (upper == null || upper.compareTo((T) value) > 0);
        return matches;

      } catch (ClassCastException e) {
        // this should never happen because we make sure that partition keys and filters
        // match the field types declared for the partitioning. But just to be sure:
        throw new IllegalArgumentException("Incompatible partition filter: condition for field '" + fieldName +
                                             "' is on " + determineClass() + " but partition key value '" + value
                                             + "' is of " + value.getClass());
      }
    }

    private Class<? extends Comparable> determineClass() {
      // either lower or upper must be non-null
      return lower != null ? lower.getClass() : upper.getClass();
    }

    @Override
    public String toString() {
      if (isSingleValue()) {
        return fieldName + "==" + lower.toString();
      } else {
        return fieldName + " in [" + (lower == null ? "null" : lower.toString())
          + "..." + (upper == null ? "null" : upper.toString()) + "]";
      }
    }
  }
}
