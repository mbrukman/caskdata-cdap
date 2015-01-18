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
package co.cask.cdap.data.stream;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.async.ExecutorUtils;
import co.cask.cdap.common.conf.PropertyChangeListener;
import co.cask.cdap.common.conf.PropertyStore;
import co.cask.cdap.common.conf.PropertyUpdater;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data2.transaction.stream.AbstractStreamFileAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

/**
 * Base implementation for {@link StreamCoordinator}.
 */
public abstract class AbstractStreamCoordinator extends AbstractIdleService implements StreamCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamCoordinator.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  // Executor for performing update action asynchronously
  private final Executor updateExecutor;
  private final StreamAdmin streamAdmin;
  private final Supplier<PropertyStore<StreamProperty>> propertyStore;
  private final Set<StreamLeaderListener> leaderListeners;

  protected AbstractStreamCoordinator(StreamAdmin streamAdmin) {
    this.streamAdmin = streamAdmin;

    propertyStore = Suppliers.memoize(new Supplier<PropertyStore<StreamProperty>>() {
      @Override
      public PropertyStore<StreamProperty> get() {
        return createPropertyStore(new StreamPropertyCodec());
      }
    });

    // Update action should be infrequent, hence just use an executor that create a new thread everytime.
    updateExecutor = ExecutorUtils.newThreadExecutor(Threads.createDaemonThreadFactory("stream-coordinator-update-%d"));

    leaderListeners = Sets.newHashSet();
  }

  /**
   * Creates a {@link PropertyStore}.
   *
   * @param codec Codec for the property stored in the property store
   * @param <T> Type of the property
   * @return A new {@link PropertyStore}.
   */
  protected abstract <T> PropertyStore<T> createPropertyStore(Codec<T> codec);

  @Override
  public ListenableFuture<Integer> nextGeneration(final StreamConfig streamConfig, final int lowerBound) {
    return Futures.transform(propertyStore.get().update(streamConfig.getName(), new PropertyUpdater<StreamProperty>() {
      @Override
      public ListenableFuture<StreamProperty> apply(@Nullable final StreamProperty property) {
        final SettableFuture<StreamProperty> resultFuture = SettableFuture.create();
        updateExecutor.execute(new Runnable() {

          @Override
          public void run() {
            try {
              long currentTTL = (property == null) ? streamConfig.getTTL() : property.getTTL();
              int newGeneration = ((property == null) ? lowerBound : property.getGeneration()) + 1;
              // Create the generation directory
              Locations.mkdirsIfNotExists(StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                                               newGeneration));
              resultFuture.set(new StreamProperty(newGeneration, currentTTL));
            } catch (IOException e) {
              resultFuture.setException(e);
            }
          }
        });
        return resultFuture;
      }
    }), new Function<StreamProperty, Integer>() {
      @Override
      public Integer apply(StreamProperty property) {
        return property.getGeneration();
      }
    });
  }

  @Override
  public ListenableFuture<Long> changeTTL(final StreamConfig streamConfig, final long newTTL) {
    return Futures.transform(propertyStore.get().update(streamConfig.getName(), new PropertyUpdater<StreamProperty>() {
      @Override
      public ListenableFuture<StreamProperty> apply(@Nullable final StreamProperty property) {
        final SettableFuture<StreamProperty> resultFuture = SettableFuture.create();
        updateExecutor.execute(new Runnable() {

          @Override
          public void run() {
            try {
              int currentGeneration = (property == null) ?
                StreamUtils.getGeneration(streamConfig) :
                property.getGeneration();

              StreamConfig newConfig = new StreamConfig(streamConfig.getName(), streamConfig.getPartitionDuration(),
                                                        streamConfig.getIndexInterval(), newTTL,
                                                        streamConfig.getLocation(),
                                                        streamConfig.getFormat());
              saveConfig(newConfig);
              resultFuture.set(new StreamProperty(currentGeneration, newTTL));
            } catch (IOException e) {
              resultFuture.setException(e);
            }
          }
        });
        return resultFuture;
      }
    }), new Function<StreamProperty, Long>() {
      @Override
      public Long apply(StreamProperty property) {
        return property.getTTL();
      }
    });
  }

  @Override
  public Cancellable addListener(String streamName, StreamPropertyListener listener) {
    return propertyStore.get().addChangeListener(streamName,
                                                 new StreamPropertyChangeListener(streamAdmin, streamName, listener));
  }

  @Override
  public Cancellable addLeaderListener(final StreamLeaderListener listener) {
    // Create a wrapper around user's listener, to ensure that the cancelling behavior set in this method
    // is not overridden by user's code implementation of the equal method
    final StreamLeaderListener wrappedListener = new StreamLeaderListener() {
      @Override
      public void leaderOf(Set<String> streamNames) {
        listener.leaderOf(streamNames);
      }
    };

    synchronized (this) {
      leaderListeners.add(wrappedListener);
    }
    return new Cancellable() {
      @Override
      public void cancel() {
        synchronized (AbstractStreamCoordinator.this) {
          leaderListeners.remove(wrappedListener);
        }
      }
    };
  }

  @Override
  protected final void shutDown() throws Exception {
    propertyStore.get().close();
    doShutDown();
  }

  /**
   * Call all the callbacks that are interested in knowing that this coordinator is the leader of a set of Streams.
   *
   * @param streamNames set of Streams that this coordinator is the leader of
   */
  protected void invokeLeaderListeners(Set<String> streamNames) {
    Set<StreamLeaderListener> callbacks;
    synchronized (this) {
      callbacks = ImmutableSet.copyOf(leaderListeners);
    }
    for (StreamLeaderListener callback : callbacks) {
      callback.leaderOf(streamNames);
    }
  }

  /**
   * Stop the service.
   *
   * @throws Exception when stopping the service could not be performed
   */
  protected abstract void doShutDown() throws Exception;

  /**
   * Overwrites a stream config file.
   *
   * @param config The new configuration.
   */
  private void saveConfig(StreamConfig config) throws IOException {
    Location configLocation = config.getLocation().append(AbstractStreamFileAdmin.CONFIG_FILE_NAME);
    Location tempLocation = configLocation.getTempFile("tmp");
    try {
      CharStreams.write(GSON.toJson(config), CharStreams.newWriterSupplier(
        Locations.newOutputSupplier(tempLocation), Charsets.UTF_8));

      Preconditions.checkState(tempLocation.renameTo(configLocation) != null,
                               "Rename {} to {} failed", tempLocation, configLocation);
    } finally {
      if (tempLocation.exists()) {
        tempLocation.delete();
      }
    }
  }

  /**
   * Object for holding property value in the property store.
   */
  private static final class StreamProperty {

    /**
     * Generation of the stream. {@code null} to ignore this field.
     */
    private final int generation;
    /**
     * TTL of the stream. {@code null} to ignore this field.
     */
    private final long ttl;

    private StreamProperty(int generation, long ttl) {
      this.generation = generation;
      this.ttl = ttl;
    }

    public int getGeneration() {
      return generation;
    }

    public long getTTL() {
      return ttl;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("generation", generation)
        .add("ttl", ttl)
        .toString();
    }
  }

  /**
   * Codec for {@link StreamProperty}.
   */
  private static final class StreamPropertyCodec implements Codec<StreamProperty> {

    private static final Gson GSON = new Gson();

    @Override
    public byte[] encode(StreamProperty property) throws IOException {
      return GSON.toJson(property).getBytes(Charsets.UTF_8);
    }

    @Override
    public StreamProperty decode(byte[] data) throws IOException {
      return GSON.fromJson(new String(data, Charsets.UTF_8), StreamProperty.class);
    }
  }

  /**
   * A {@link PropertyChangeListener} that convert onChange callback into {@link StreamPropertyListener}.
   */
  private static final class StreamPropertyChangeListener extends StreamPropertyListener
                                                          implements PropertyChangeListener<StreamProperty> {

    private final StreamPropertyListener listener;
    // Callback from PropertyStore is
    private StreamProperty currentProperty;

    private StreamPropertyChangeListener(StreamAdmin streamAdmin, String streamName, StreamPropertyListener listener) {
      this.listener = listener;
      try {
        StreamConfig streamConfig = streamAdmin.getConfig(streamName);
        this.currentProperty = new StreamProperty(StreamUtils.getGeneration(streamConfig), streamConfig.getTTL());
      } catch (Exception e) {
        // It's ok if the stream config is not yet available (meaning no data has ever been writen to the stream yet.
        this.currentProperty = new StreamProperty(0, Long.MAX_VALUE);
      }
    }

    @Override
    public void onChange(String name, StreamProperty newProperty) {
      try {
        if (newProperty != null) {
          if (currentProperty == null || currentProperty.getGeneration() < newProperty.getGeneration()) {
            generationChanged(name, newProperty.getGeneration());
          }

          if (currentProperty == null || currentProperty.getTTL() != newProperty.getTTL()) {
            ttlChanged(name, newProperty.getTTL());
          }
        } else {
          generationDeleted(name);
          ttlDeleted(name);
        }
      } finally {
        currentProperty = newProperty;
      }
    }

    @Override
    public void onError(String name, Throwable failureCause) {
      LOG.error("Exception on PropertyChangeListener for stream {}", name, failureCause);
    }

    @Override
    public void generationChanged(String streamName, int generation) {
      try {
        listener.generationChanged(streamName, generation);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.generationChanged", t);
      }
    }

    @Override
    public void generationDeleted(String streamName) {
      try {
        listener.generationDeleted(streamName);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.generationDeleted", t);
      }
    }

    @Override
    public void ttlChanged(String streamName, long ttl) {
      try {
        listener.ttlChanged(streamName, ttl);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.ttlChanged", t);
      }
    }

    @Override
    public void ttlDeleted(String streamName) {
      try {
        listener.ttlDeleted(streamName);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.ttlDeleted", t);
      }
    }
  }
}
