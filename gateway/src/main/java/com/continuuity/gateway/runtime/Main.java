package com.continuuity.gateway.runtime;

import com.continuuity.app.guice.LocationRuntimeModule;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.service.ServerException;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.executor.remote.Constants;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.Gateway;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.logging.runtime.LoggingModules;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Main is a simple class that allows us to launch the Gateway as a standalone
 * program. This is also where we do our runtime injection.
 * <p/>
 */
public class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  /**
   * Our main method.
   *
   * @param args Our command line options
   */
  public static void main(String[] args) {
    // Load our configuration from our resource files
    CConfiguration configuration = CConfiguration.create();

    String zookeeper = configuration.get(Constants.CFG_ZOOKEEPER_ENSEMBLE);
    if (zookeeper == null) {
      LOG.error("No zookeeper quorum provided.");
      System.exit(1);
    }
    final ZKClientService zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(zookeeper).build(),
            RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
          )
        ));
    zkClientService.startAndWait();

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
        new GatewayModules(configuration).getDistributedModules(),
        new DataFabricModules().getDistributedModules(),
        new ConfigModule(configuration),
        new IOModule(),
        new LocationRuntimeModule().getDistributedModules(),
        new DiscoveryRuntimeModule(zkClientService).getDistributedModules(),
        new LoggingModules().getDistributedModules(),
        new AbstractModule() {
          @Override
          protected void configure() {
            // It's a bit hacky to add it here. Need to refactor these bindings out as it overlaps with
            // AppFabricServiceModule
            bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
            bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
            bind(StoreFactory.class).to(MDSStoreFactory.class);
          }
        }
        );

    // Get our fully wired Gateway
    final Gateway theGateway = injector.getInstance(Gateway.class);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          theGateway.stop(true);
        } catch (ServerException e) {
          LOG.error("Caught exception while trying to stop Gateway", e);
        }
        zkClientService.stopAndWait();
      }
    });

    // Now, initialize the Gateway
    try {

      // Start the gateway!
      theGateway.start(null, configuration);

    } catch (Exception e) {
      LOG.error(e.toString(), e);
      System.exit(-1);
    }
  }

} // End of Main

