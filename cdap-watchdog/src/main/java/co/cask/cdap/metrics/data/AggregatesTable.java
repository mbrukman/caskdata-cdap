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

package co.cask.cdap.metrics.data;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.transport.MetricType;
import co.cask.cdap.metrics.transport.MetricsRecord;
import co.cask.cdap.metrics.transport.TagMetric;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Table for storing aggregated metrics for all time.
 *
 * <p>
 *   Row key:
 *   {@code context|metric|runId}
 * </p>
 * <p>
 *   Column:
 *   {@code tag}
 * </p>
 */
public final class AggregatesTable {

  private final MetricsEntityCodec entityCodec;
  private final MetricsTable aggregatesTable;

  AggregatesTable(MetricsTable aggregatesTable, MetricsEntityCodec entityCodec) {
    this.entityCodec = entityCodec;
    this.aggregatesTable = aggregatesTable;
  }

  /**
   * Updates aggregates for the given list of {@link MetricsRecord}.
   *
   * @throws Exception When there is an error updating the table.
   */
  public void update(Iterable<MetricsRecord> records) throws Exception {
    update(records.iterator());
  }

  /**
   * Updates aggregates for the given iterator of {@link MetricsRecord}.
   *
   * @throws Exception When there is an error updating the table.
   */
  public void update(Iterator<MetricsRecord> records) throws Exception {
    while (records.hasNext()) {
      MetricsRecord record = records.next();
      byte[] rowKey = getKey(record.getContext(), record.getName(), record.getRunId());
      if (record.getType() == MetricType.COUNTER) {
        Map<byte[], Long> increments = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

        // The no tag value
        increments.put(Bytes.toBytes(MetricsConstants.EMPTY_TAG), record.getValue());

        // For each tag, increments corresponding values
        for (TagMetric tag : record.getTags()) {
          increments.put(Bytes.toBytes(tag.getTag()), tag.getValue());
        }
        aggregatesTable.increment(rowKey, increments);
      } else if (record.getType() == MetricType.GAUGE) {
        NavigableMap<byte[], Long> gauges = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        // The no tag value
        gauges.put(Bytes.toBytes(MetricsConstants.EMPTY_TAG), record.getValue());

        // For each tag, sets corresponding values
        for (TagMetric tag : record.getTags()) {
          gauges.put(Bytes.toBytes(tag.getTag()), record.getValue());
        }
        NavigableMap<byte[], NavigableMap<byte[], Long>> keyMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        keyMap.put(rowKey, gauges);
        aggregatesTable.put(keyMap);
      }
    }
  }

  /**
   * Deletes all the row keys which match the context prefix.
   * @param contextPrefix Prefix of the context to match.  Null not allowed, full table deletes should be done through
   *                      the clear method.
   * @throws Exception if there is an error in deleting entries.
   */
  public void delete(String contextPrefix) throws Exception {
    Preconditions.checkArgument(contextPrefix != null, "null context not allowed");
    aggregatesTable.deleteAll(entityCodec.encodeWithoutPadding(MetricsEntityType.CONTEXT, contextPrefix));
  }

  /**
   * Deletes all the row keys which match the context prefix and metric prefix.  Context and Metric cannot both be
   * null, as full table deletes should be done through the clear method.
   *
   * @param contextPrefix Prefix of the context to match, null means any context.
   * @param metricPrefix Prefix of the metric to match, null means any metric.
   * @throws Exception if there is an error in deleting entries.
   */
  public void delete(String contextPrefix, String metricPrefix) throws Exception {
    Preconditions.checkArgument(contextPrefix != null || metricPrefix != null,
                                "context and metric cannot both be null");
    if (metricPrefix == null) {
      delete(contextPrefix);
    } else {
      delete(contextPrefix, metricPrefix, "0", (String[]) null);
    }
  }

  /**
   * Deletes entries in the aggregate table that match the given context prefix, metric prefix, runId, and tag.
   *
   * @param contextPrefix Prefix of context to match, null means any context.
   * @param metricPrefix Prefix of metric to match, null means any metric.
   * @param runId Runid to match.
   * @param tags Tags to match, null means any tag.
   * @throws Exception if there is an error in deleting entries.
   */
  public void delete(String contextPrefix, String metricPrefix, String runId, String... tags)
    throws Exception {
    byte[] startRow = getRawPaddedKey(contextPrefix, metricPrefix, runId, 0);
    byte[] endRow = getRawPaddedKey(contextPrefix, metricPrefix, runId, 0xff);
    aggregatesTable.deleteRange(startRow, endRow, tags == null ? null : Bytes.toByteArrays(tags),
                                getFilter(contextPrefix, metricPrefix, runId));
  }

  /**
   * Scans the aggregate table for metrics without tag.
   * @param contextPrefix Prefix of context to match, null means any context.
   * @param metricPrefix Prefix of metric to match, null means any metric.
   * @return A {@link AggregatesScanner} for iterating scan over matching rows
   */
  public AggregatesScanner scan(String contextPrefix, String metricPrefix) {
    return scan(contextPrefix, metricPrefix, null, MetricsConstants.EMPTY_TAG);
  }

  /**
   * Scans the aggregate table.
   *
   * @param contextPrefix Prefix of context to match, null means any context.
   * @param metricPrefix Prefix of metric to match, null means any metric.
   * @param tagPrefix Prefix of tag to match. A null value will match untagged metrics, which is the same as passing in
   *                  MetricsConstants.EMPTY_TAG.
   * @return A {@link AggregatesScanner} for iterating scan over matching rows
   */
  public AggregatesScanner scan(String contextPrefix, String metricPrefix, String runId, String tagPrefix) {
    return scanFor(contextPrefix, metricPrefix, runId, tagPrefix == null ? MetricsConstants.EMPTY_TAG : tagPrefix);
  }

  /**
   * Scans the aggregate table for the given context and metric prefixes, and across all tags
   * including the empty tag.  Potentially expensive, use of this method should be avoided if the tag wanted is
   * known before hand.
   *
   * @param contextPrefix Prefix of context to match, a null value means any context.
   * @param metricPrefix Prefix of metric to match, a null value means any metric.
   * @return A {@link AggregatesScanner} for iterating scan over matching rows.
   */
  public AggregatesScanner scanAllTags(String contextPrefix, String metricPrefix) {
    return scanFor(contextPrefix, metricPrefix, "0", null);
  }

  /**
   * Scans the aggregate table for its rows only.
   *
   * @param contextPrefix Prefix of context to match, null means any context.
   * @param metricPrefix Prefix of metric to match, null means any metric.
   * @return A {@link AggregatesScanner} for iterating scan over matching rows
   */
  public AggregatesScanner scanRowsOnly(String contextPrefix, String metricPrefix) {
    return scanRowsOnly(contextPrefix, metricPrefix, null, MetricsConstants.EMPTY_TAG);
  }

  /**
   * Scans the aggregate table for its rows only.
   *
   * @param contextPrefix Prefix of context to match, null means any context.
   * @param metricPrefix Prefix of metric to match, null means any metric.
   * @param tagPrefix Prefix of tag to match.  A null value will match untagged metrics, which is the same as passing in
   *                  MetricsConstants.EMPTY_TAG.
   * @return A {@link AggregatesScanner} for iterating scan over matching rows
   */
  public AggregatesScanner scanRowsOnly(String contextPrefix, String metricPrefix, String runId, String tagPrefix) {
    byte[] startRow = getRawPaddedKey(contextPrefix, metricPrefix, runId, 0);
    byte[] endRow = getRawPaddedKey(contextPrefix, metricPrefix, runId, 0xff);

    try {
      Scanner scanner = aggregatesTable.scan(startRow, endRow, null, getFilter(contextPrefix, metricPrefix, runId));
      return new AggregatesScanner(contextPrefix, metricPrefix, runId,
                                   tagPrefix == null ? MetricsConstants.EMPTY_TAG : tagPrefix, scanner, entityCodec);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Clears the storage table.
   * @throws Exception If error in clearing data.
   */
  public void clear() throws Exception {
    aggregatesTable.deleteAll(new byte[] { });
  }

  private AggregatesScanner scanFor(String contextPrefix, String metricPrefix, String runId, String tagPrefix) {
    byte[] startRow = getPaddedKey(contextPrefix, metricPrefix, runId, 0);
    byte[] endRow = getPaddedKey(contextPrefix, metricPrefix, runId, 0xff);
    try {
      // scan starting from start to end across all columns using a fuzzy filter for efficiency
      Scanner scanner = aggregatesTable.scan(startRow, endRow, null, getFilter(contextPrefix, metricPrefix, runId));
      return new AggregatesScanner(contextPrefix, metricPrefix, runId, tagPrefix, scanner, entityCodec);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private byte[] getKey(String context, String metric, String runId) {
    Preconditions.checkArgument(context != null, "Context cannot be null.");
    Preconditions.checkArgument(runId != null, "RunId cannot be null.");
    Preconditions.checkArgument(metric != null, "Metric cannot be null.");

    return Bytes.add(
      entityCodec.encode(MetricsEntityType.CONTEXT, context),
      entityCodec.encode(MetricsEntityType.METRIC, metric),
      entityCodec.encode(MetricsEntityType.RUN, runId)
    );
  }

  private byte[] getPaddedKey(String contextPrefix, String metricPrefix, String runId, int padding) {

    Preconditions.checkArgument(metricPrefix != null, "Metric cannot be null.");

    return getRawPaddedKey(contextPrefix, metricPrefix, runId, padding);
  }

  private byte[] getRawPaddedKey(String contextPrefix, String metricPrefix, String runId, int padding) {
    return Bytes.concat(
      entityCodec.paddedEncode(MetricsEntityType.CONTEXT, contextPrefix, padding),
      entityCodec.paddedEncode(MetricsEntityType.METRIC, metricPrefix, padding),
      entityCodec.paddedEncode(MetricsEntityType.RUN, runId, padding)
    );
  }

  private FuzzyRowFilter getFilter(String contextPrefix, String metricPrefix, String runId) {
    // Create fuzzy row filter
    ImmutablePair<byte[], byte[]> contextPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.CONTEXT,
                                                                              contextPrefix, 0);
    ImmutablePair<byte[], byte[]> metricPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.METRIC,
                                                                             metricPrefix, 0);
    ImmutablePair<byte[], byte[]> runIdPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.RUN, runId, 0);

    // Use a FuzzyRowFilter to select the row and the use ColumnPrefixFilter to select tag column.
    return new FuzzyRowFilter(ImmutableList.of(ImmutablePair.of(
      Bytes.concat(contextPair.getFirst(), metricPair.getFirst(), runIdPair.getFirst()),
      Bytes.concat(contextPair.getSecond(), metricPair.getSecond(), runIdPair.getSecond())
    )));
  }
}
