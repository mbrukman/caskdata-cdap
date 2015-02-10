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

package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * admin for streams in hbase.
 */
@Singleton
public class HBaseStreamAdmin extends HBaseQueueAdmin implements StreamAdmin {

  @Inject
  public HBaseStreamAdmin(Configuration hConf, CConfiguration cConf, LocationFactory locationFactory,
                          HBaseTableUtil tableUtil) throws IOException {
    super(hConf, cConf, QueueConstants.QueueType.STREAM, locationFactory, tableUtil);
  }

  @Override
  public String getActualTableName(QueueName queueName) {
    if (queueName.isStream()) {
      //TODO: namespace the tableName (have to modify tableNamePrefix)
      // <cdap namespace>.system.stream.<stream name>
      return getTableNamePrefix() + "." + queueName.getSecondComponent();
    } else {
      throw new IllegalArgumentException("'" + queueName + "' is not a valid name for a stream.");
    }
  }

  @Override
  public boolean doDropTable(QueueName queueName) {
    // separate table for each stream, ok to drop
    return true;
  }

  @Override
  public boolean doTruncateTable(QueueName queueName) {
    // separate table for each stream, ok to truncate
    return true;
  }

  @Override
  protected List<? extends Class<? extends Coprocessor>> getCoprocessors() {
    // we don't want eviction CP here, hence overriding
    return ImmutableList.of(tableUtil.getDequeueScanObserverClassForVersion());
  }

  @Override
  public void configureInstances(Id.Stream streamId, long groupId, int instances) throws Exception {
    configureInstances(QueueName.fromStream(streamId), groupId, instances);
  }

  @Override
  public void configureGroups(Id.Stream streamId, Map<Long, Integer> groupInfo) throws Exception {
    configureGroups(QueueName.fromStream(streamId), groupInfo);
  }

  @Override
  public StreamConfig getConfig(Id.Stream streamId) throws IOException {
    return null;
  }

  @Override
  public void updateConfig(StreamConfig config) throws IOException {

  }

  @Override
  public long fetchStreamSize(StreamConfig streamConfig) throws IOException {
    return 0;
  }

  private String fromStream(Id.Stream streamId) {
    return QueueName.fromStream(streamId).toURI().toString();
  }

  @Override
  public boolean exists(Id.Stream streamId) throws Exception {
    return exists(fromStream(streamId));
  }

  @Override
  public void create(Id.Stream streamId) throws Exception {
    create(fromStream(streamId));
  }

  @Override
  public void create(Id.Stream streamId, @Nullable Properties props) throws Exception {
    create(fromStream(streamId), props);
  }

  @Override
  public void truncate(Id.Stream streamId) throws Exception {
    truncate(fromStream(streamId));
  }

  @Override
  public void drop(Id.Stream streamId) throws Exception {
    drop(fromStream(streamId));
  }

}
