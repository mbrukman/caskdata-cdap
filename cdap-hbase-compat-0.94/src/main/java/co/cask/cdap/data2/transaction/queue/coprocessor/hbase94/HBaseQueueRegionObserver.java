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
package co.cask.cdap.data2.transaction.queue.coprocessor.hbase94;

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.queue.ConsumerEntryState;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.QueueUtils;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueAdmin;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.ConsumerConfigCache;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.ConsumerInstance;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.QueueConsumerConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * RegionObserver for queue table. This class should only have JSE and HBase classes dependencies only.
 * It can also has dependencies on CDAP classes provided that all the transitive dependencies stay within
 * the mentioned scope.
 *
 * This region observer does queue eviction during flush time and compact time by using queue consumer state
 * information to determine if a queue entry row can be omitted during flush/compact.
 */
public final class HBaseQueueRegionObserver extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(HBaseQueueRegionObserver.class);

  private ConsumerConfigCache configCache;

  private int prefixBytes;
  private String namespaceId;
  private String appName;
  private String flowName;

  @Override
  public void start(CoprocessorEnvironment env) {
    if (env instanceof RegionCoprocessorEnvironment) {
      HTableDescriptor tableDesc = ((RegionCoprocessorEnvironment) env).getRegion().getTableDesc();
      String tableName = tableDesc.getNameAsString();
      String configTableName = QueueUtils.determineQueueConfigTableName(tableName);

      String prefixBytes = tableDesc.getValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES);
      try {
        // Default to SALT_BYTES for the older salted queue implementation.
        this.prefixBytes = prefixBytes == null ? HBaseQueueAdmin.SALT_BYTES : Integer.parseInt(prefixBytes);
      } catch (NumberFormatException e) {
        // Shouldn't happen for table created by cdap.
        LOG.error("Unable to parse value of '" + HBaseQueueAdmin.PROPERTY_PREFIX_BYTES + "' property. " +
                  "Default to " + HBaseQueueAdmin.SALT_BYTES, e);
        this.prefixBytes = HBaseQueueAdmin.SALT_BYTES;
      }

      namespaceId = HBaseQueueAdmin.getNamespaceId(tableName);
      appName = HBaseQueueAdmin.getApplicationName(tableName);
      flowName = HBaseQueueAdmin.getFlowName(tableName);

      configCache = ConsumerConfigCache.getInstance(env.getConfiguration(),
                                                    Bytes.toBytes(configTableName));
    }
  }

  @Override
  public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e,
                                  Store store, InternalScanner scanner) throws IOException {
    if (!e.getEnvironment().getRegion().isAvailable()) {
      return scanner;
    }

    LOG.info("preFlush, creates EvictionInternalScanner");
    return new EvictionInternalScanner("flush", e.getEnvironment(), scanner);
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
                                    Store store, InternalScanner scanner) throws IOException {
    if (!e.getEnvironment().getRegion().isAvailable()) {
      return scanner;
    }

    LOG.info("preCompact, creates EvictionInternalScanner");
    return new EvictionInternalScanner("compaction", e.getEnvironment(), scanner);
  }

  // needed for queue unit-test
  private ConsumerConfigCache getConfigCache() {
    return configCache;
  }

  /**
   * An {@link InternalScanner} that will skip queue entries that are safe to be evicted.
   */
  private final class EvictionInternalScanner implements InternalScanner {

    private final String triggeringAction;
    private final RegionCoprocessorEnvironment env;
    private final InternalScanner scanner;
    // This is just for object reused to reduce objects creation.
    private final ConsumerInstance consumerInstance;
    private byte[] currentQueue;
    private byte[] currentQueueRowPrefix;
    private QueueConsumerConfig consumerConfig;
    private long totalRows = 0;
    private long rowsEvicted = 0;
    // couldn't be evicted due to incomplete view of row
    private long skippedIncomplete = 0;

    private EvictionInternalScanner(String action, RegionCoprocessorEnvironment env, InternalScanner scanner) {
      this.triggeringAction = action;
      this.env = env;
      this.scanner = scanner;
      this.consumerInstance = new ConsumerInstance(0, 0);
    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
      return next(results, -1, null);
    }

    @Override
    public boolean next(List<KeyValue> results, String metric) throws IOException {
      return next(results, -1, metric);
    }

    @Override
    public boolean next(List<KeyValue> results, int limit) throws IOException {
      return next(results, limit, null);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
      boolean hasNext = scanner.next(result, limit, metric);

      while (!result.isEmpty()) {
        totalRows++;
        // Check if it is eligible for eviction.
        KeyValue kv = result.get(0);

        // If current queue is unknown or the row is not a queue entry of current queue,
        // it either because it scans into next queue entry or simply current queue is not known.
        // Hence needs to find the currentQueue
        if (currentQueue == null || !QueueEntryRow.isQueueEntry(currentQueueRowPrefix, prefixBytes, kv.getBuffer(),
                                                                kv.getRowOffset(), kv.getRowLength())) {
          // If not eligible, it either because it scans into next queue entry or simply current queue is not known.
          currentQueue = null;
        }

        // This row is a queue entry. If currentQueue is null, meaning it's a new queue encountered during scan.
        if (currentQueue == null) {
          QueueName queueName = QueueEntryRow.getQueueName(namespaceId,
                                                           appName, flowName, prefixBytes,
                                                           kv.getBuffer(), kv.getRowOffset(), kv.getRowLength());
          currentQueue = queueName.toBytes();
          currentQueueRowPrefix = QueueEntryRow.getQueueRowPrefix(queueName);
          consumerConfig = configCache.getConsumerConfig(currentQueue);
        }

        if (consumerConfig == null) {
          // no config is present yet, so cannot evict
          return hasNext;
        }

        if (canEvict(consumerConfig, result)) {
          rowsEvicted++;
          result.clear();
          hasNext = scanner.next(result, limit, metric);
        } else {
          break;
        }
      }

      return hasNext;
    }

    @Override
    public void close() throws IOException {
      LOG.info("Region " + env.getRegion().getRegionNameAsString() + " " + triggeringAction +
                 ", rows evicted: " + rowsEvicted + " / " + totalRows + ", skipped incomplete: " + skippedIncomplete);
      scanner.close();
    }

    /**
     * Determines the given queue entry row can be evicted.
     * @param result All KeyValues of a queue entry row.
     * @return true if it can be evicted, false otherwise.
     */
    private boolean canEvict(QueueConsumerConfig consumerConfig, List<KeyValue> result) {
      // If no consumer group, this queue is dead, should be ok to evict.
      if (consumerConfig.getNumGroups() == 0) {
        return true;
      }

      // If unknown consumer config (due to error), keep the queue.
      if (consumerConfig.getNumGroups() < 0) {
        return false;
      }

      // TODO (terence): Right now we can only evict if we see all the data columns.
      // It's because it's possible that in some previous flush, only the data columns are flush,
      // then consumer writes the state columns. In the next flush, it'll only see the state columns and those
      // should not be evicted otherwise the entry might get reprocessed, depending on the consumer start row state.
      // This logic is not perfect as if flush happens after enqueue and before dequeue, that entry may never get
      // evicted (depends on when the next compaction happens, whether the queue configuration has been change or not).

      // There are two data columns, "d" and "m".
      // If the size == 2, it should not be evicted as well,
      // as state columns (dequeue) always happen after data columns (enqueue).
      if (result.size() <= 2) {
        skippedIncomplete++;
        return false;
      }

      // "d" and "m" columns always comes before the state columns, prefixed with "s".
      Iterator<KeyValue> iterator = result.iterator();
      if (!QueueEntryRow.isDataColumn(iterator.next())) {
        skippedIncomplete++;
        return false;
      }
      if (!QueueEntryRow.isMetaColumn(iterator.next())) {
        skippedIncomplete++;
        return false;
      }

      // Need to determine if this row can be evicted iff all consumer groups have committed process this row.
      int consumedGroups = 0;
      // Inspect each state column
      while (iterator.hasNext()) {
        KeyValue kv = iterator.next();
        if (!QueueEntryRow.isStateColumn(kv)) {
          continue;
        }
        // If any consumer has a state != PROCESSED, it should not be evicted
        if (!isProcessed(kv, consumerInstance)) {
          break;
        }
        // If it is PROCESSED, check if this row is smaller than the consumer instance startRow.
        // Essentially a loose check of committed PROCESSED.
        byte[] startRow = consumerConfig.getStartRow(consumerInstance);
        if (startRow != null && compareRowKey(kv, startRow) < 0) {
          consumedGroups++;
        }
      }

      // It can be evicted if from the state columns, it's been processed by all consumer groups
      // Otherwise, this row has to be less than smallest among all current consumers.
      // The second condition is for handling consumer being removed after it consumed some entries.
      // However, the second condition alone is not good enough as it's possible that in hash partitioning,
      // only one consumer is keep consuming when the other consumer never proceed.
      return consumedGroups == consumerConfig.getNumGroups()
          || compareRowKey(result.get(0), consumerConfig.getSmallestStartRow()) < 0;
    }

    private int compareRowKey(KeyValue kv, byte[] row) {
      return Bytes.compareTo(kv.getBuffer(), kv.getRowOffset() + prefixBytes,
                             kv.getRowLength() - prefixBytes, row, 0, row.length);
    }

    /**
     * Returns {@code true} if the given {@link KeyValue} has a {@link ConsumerEntryState#PROCESSED} state and
     * also put the consumer information into the given {@link ConsumerInstance}.
     * Otherwise, returns {@code false} and the {@link ConsumerInstance} is left untouched.
     */
    private boolean isProcessed(KeyValue keyValue, ConsumerInstance consumerInstance) {
      byte[] buffer = keyValue.getBuffer();
      int stateIdx = keyValue.getValueOffset() + keyValue.getValueLength() - 1;
      boolean processed = buffer[stateIdx] == ConsumerEntryState.PROCESSED.getState();

      if (processed) {
        // Column is "s<groupId>"
        long groupId = Bytes.toLong(buffer, keyValue.getQualifierOffset() + 1);
        // Value is "<writePointer><instanceId><state>"
        int instanceId = Bytes.toInt(buffer, keyValue.getValueOffset() + Bytes.SIZEOF_LONG);
        consumerInstance.setGroupInstance(groupId, instanceId);
      }
      return processed;
    }
  }
}
