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

  private final Map<String, Condition<? extends Comparable>> filters;

  // we only allow creating an filter through the builder.
  private PartitionFilter(Map<String, Condition<? extends Comparable>> filters) {
    this.filters = filters;
  }

  /**
   * Returns the individual conditions of this filter.
   * This should be used for inspection or debugging only.
   * To match this filter, use the {@link #match} method.
   */
  public Map<String, Condition<? extends Comparable>> getFilters() {
    return ImmutableMap.copyOf(filters);
  }

  /**
   * Match this filter against a partition key. The key matches iff it matches all conditions.
   */
  public boolean match(PartitionKey partitionKey) {
    for (Map.Entry<String, Condition<? extends Comparable>> condition : getFilters().entrySet())  {
      Comparable value = partitionKey.getField(condition.getKey());
      if (!condition.getValue().match(value)) {
        return false;
      }
    }
    return true;
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
     * Either bound can be null, meaning unbounded in that direction.
     * @param field The name of the partition field
     * @param lower the inclusive lower bound. If null, there is no lower bound.
     * @param upper the exclusive upper bound. If null, there is no upper bound.
     * @param <T> The type of the partition field
     */
    public <T extends Comparable<T>> Builder addRangeCondition(String field,
                                                               @Nullable T lower,
                                                               @Nullable T upper) {
      if (null == lower && null == upper) { // filter is pointless if there is no bound
        return this;
      }
      if (lower != null && lower.equals(upper)) { // filter can't be satisfied if lower equals upper
        throw new IllegalArgumentException(
          "Unsatisfiable condition: lower bound and upper bound have equal value: '" + lower + "'");
      }
      map.put(field, new Condition<T>(lower, upper));
      return this;
    }

    /**
     * Add a condition that matches by equality.
     * @param field The name of the partition field
     * @param value The value that matching field values must have
     * @param <T> The type of the partition field
     */
    public <T extends Comparable<T>> Builder addValueCondition(String field, T value) {
      if (value != null) {
        map.put(field, new Condition<T>(value, value));
      }
      return this;
    }

    /**
     * Create the PartitionFilter.
     */
    public PartitionFilter build() {
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

    private final T lower;
    private final T upper;

    private Condition(T lower, T upper) {
      Preconditions.checkArgument(lower != null || upper != null, "Either lower or upper-bound must be non-null.");
      this.lower = lower;
      this.upper = upper;
    }

    public T getLower() {
      return lower;
    }

    public T getUpper() {
      return upper;
    }

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
        throw new IllegalArgumentException("Can't compare value '" + value
                                             + "' as an object of class " + determineClass());
      }
    }

    private Class<? extends Comparable> determineClass() {
      // either lower or upper must be non-null
      return lower != null ? lower.getClass() : upper.getClass();
    }

  }
}
