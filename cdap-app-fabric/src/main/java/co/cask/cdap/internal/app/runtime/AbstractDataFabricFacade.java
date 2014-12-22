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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data.dataset.DatasetInstantiator;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;
import co.cask.cdap.data2.transaction.queue.hbase.ShardedQueueProducerFactory;
import co.cask.cdap.data2.transaction.stream.ForwardingStreamConsumer;
import co.cask.cdap.data2.transaction.stream.StreamConsumer;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * Abstract base class for implementing DataFabricFacade.
 */
public abstract class AbstractDataFabricFacade implements DataFabricFacade, ShardedQueueProducerFactory {

  private final DatasetInstantiator dataSetContext;
  private final QueueClientFactory queueClientFactory;
  private final StreamConsumerFactory streamConsumerFactory;
  private final TransactionExecutorFactory txExecutorFactory;
  private final TransactionSystemClient txSystemClient;
  private final Id.Program programId;

  public AbstractDataFabricFacade(TransactionSystemClient txSystemClient, TransactionExecutorFactory txExecutorFactory,
                                  QueueClientFactory queueClientFactory, StreamConsumerFactory streamConsumerFactory,
                                  Program program, DatasetInstantiator dataSetContext) {
    this.txSystemClient = txSystemClient;
    this.queueClientFactory = queueClientFactory;
    this.streamConsumerFactory = streamConsumerFactory;
    this.txExecutorFactory = txExecutorFactory;
    this.dataSetContext = dataSetContext;
    this.programId = program.getId();
  }

  @Override
  public DatasetContext getDataSetContext() {
    return dataSetContext;
  }

  @Override
  public TransactionContext createTransactionManager() {
    return new TransactionContext(txSystemClient, dataSetContext.getTransactionAware());
  }

  @Override
  public TransactionExecutor createTransactionExecutor() {
    return txExecutorFactory.createExecutor(dataSetContext.getTransactionAware());
  }

  @Override
  public QueueProducer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public QueueConsumer createConsumer(QueueName queueName,
                                       ConsumerConfig consumerConfig, int numGroups) throws IOException {
    QueueConsumer consumer = queueClientFactory.createConsumer(queueName, consumerConfig, numGroups);
    if (consumer instanceof TransactionAware) {
      consumer = new CloseableQueueConsumer(dataSetContext, consumer);
      dataSetContext.addTransactionAware((TransactionAware) consumer);
    }
    return consumer;
  }

  @Override
  public QueueProducer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    QueueProducer producer = queueClientFactory.createProducer(queueName, queueMetrics);
    if (producer instanceof TransactionAware) {
      dataSetContext.addTransactionAware((TransactionAware) producer);
    }
    return producer;
  }

  @Override
  public QueueProducer createProducer(QueueName queueName, QueueMetrics queueMetrics,
                                      Iterable<ConsumerConfig> consumerConfigs) throws IOException {
    // The underlying queue client factory has to be sharded one.
    Preconditions.checkState(queueClientFactory instanceof ShardedQueueProducerFactory,
                             "Sharded queue not supported");

    QueueProducer producer = ((ShardedQueueProducerFactory) queueClientFactory).createProducer(queueName,
                                                                                               queueMetrics,
                                                                                               consumerConfigs);
    if (producer instanceof TransactionAware) {
      dataSetContext.addTransactionAware((TransactionAware) producer);
    }
    return producer;
  }

  @Override
  public StreamConsumer createStreamConsumer(QueueName streamName, ConsumerConfig consumerConfig) throws IOException {
    String namespace = String.format("%s.%s", programId.getApplicationId(), programId.getId());
    final StreamConsumer consumer = streamConsumerFactory.create(streamName, namespace, consumerConfig);

    dataSetContext.addTransactionAware(consumer);
    return new ForwardingStreamConsumer(consumer) {
      @Override
      public void close() throws IOException {
        super.close();
        dataSetContext.removeTransactionAware(consumer);
      }
    };
  }
}
