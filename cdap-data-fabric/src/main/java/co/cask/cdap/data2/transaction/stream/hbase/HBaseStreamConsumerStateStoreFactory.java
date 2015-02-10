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
package co.cask.cdap.data2.transaction.stream.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStore;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Factory for creating {@link StreamConsumerStateStore} in HBase.
 */
public final class HBaseStreamConsumerStateStoreFactory implements StreamConsumerStateStoreFactory {

  private final Configuration hConf;
  private final HBaseTableUtil tableUtil;

  @Inject
  HBaseStreamConsumerStateStoreFactory(Configuration hConf, HBaseTableUtil tableUtil) {
    this.hConf = hConf;
    this.tableUtil = tableUtil;
  }

  @Override
  public synchronized StreamConsumerStateStore create(StreamConfig streamConfig) throws IOException {
    Id.Namespace namespace = streamConfig.getStreamId().getNamespace();
    byte[] tableName = Bytes.toBytes(constructTableName(namespace));

    HBaseAdmin admin = new HBaseAdmin(hConf);
    if (!admin.tableExists(tableName)) {
      try {
        HTableDescriptor htd = new HTableDescriptor(tableName);

        HColumnDescriptor hcd = new HColumnDescriptor(QueueEntryRow.COLUMN_FAMILY);
        htd.addFamily(hcd);
        hcd.setMaxVersions(1);

        tableUtil.createTableIfNotExists(admin, tableName, htd, null,
                                         QueueConstants.MAX_CREATE_TABLE_WAIT, TimeUnit.MILLISECONDS);
      } finally {
        admin.close();
      }
    }

    HTable hTable = new HTable(hConf, tableName);
    hTable.setWriteBufferSize(Constants.Stream.HBASE_WRITE_BUFFER_SIZE);
    hTable.setAutoFlush(false);
    return new HBaseStreamConsumerStateStore(streamConfig, hTable);
  }

  @Override
  public synchronized void dropAllInNamespace(Id.Namespace namespace) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(hConf);
    try {
      byte[] tableName = Bytes.toBytes(constructTableName(namespace));

      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }
    } finally {
      admin.close();
    }
  }

  private String constructTableName(Id.Namespace namespace) {
    return HBaseTableUtil.getHBaseTableName(StreamUtils.constructStateStoreTableName(namespace));
  }
}
