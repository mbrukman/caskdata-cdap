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

package co.cask.cdap;

import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.app.store.ServiceStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.utils.OSDetector;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.service.StreamHttpService;
import co.cask.cdap.data.stream.service.StreamServiceRuntimeModule;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.executor.ExploreExecutorService;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.explore.service.ExploreServiceUtils;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.gateway.router.NettyRouter;
import co.cask.cdap.gateway.router.RouterModules;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.security.guice.SecurityModules;
import co.cask.cdap.security.server.ExternalAuthenticationServer;
import co.cask.tephra.inmemory.InMemoryTransactionService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintStream;
import java.net.InetAddress;
import java.util.List;

/**
 * Standalone Main.
 * NOTE: Use AbstractIdleService
 */
public class StandaloneMain {
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneMain.class);

  private final WebCloudAppService webCloudAppService;
  private final NettyRouter router;
  private final MetricsQueryService metricsQueryService;
  private final AppFabricServer appFabricServer;
  private final StreamHttpService streamHttpService;
  private final ServiceStore serviceStore;

  private final MetricsCollectionService metricsCollectionService;

  private final LogAppenderInitializer logAppenderInitializer;
  private final InMemoryTransactionService txService;
  private final boolean securityEnabled;
  private final boolean sslEnabled;
  private final CConfiguration configuration;

  private ExternalAuthenticationServer externalAuthenticationServer;
  private final DatasetService datasetService;

  private ExploreExecutorService exploreExecutorService;
  private final ExploreClient exploreClient;

  private StandaloneMain(List<Module> modules, CConfiguration configuration, String webAppPath) {
    this.configuration = configuration;

    Injector injector = Guice.createInjector(modules);
    txService = injector.getInstance(InMemoryTransactionService.class);
    router = injector.getInstance(NettyRouter.class);
    metricsQueryService = injector.getInstance(MetricsQueryService.class);
    appFabricServer = injector.getInstance(AppFabricServer.class);
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);

    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    datasetService = injector.getInstance(DatasetService.class);
    serviceStore = injector.getInstance(ServiceStore.class);

    this.webCloudAppService = (webAppPath == null) ? null : injector.getInstance(WebCloudAppService.class);

    streamHttpService = injector.getInstance(StreamHttpService.class);

    sslEnabled = configuration.getBoolean(Constants.Security.SSL_ENABLED);
    securityEnabled = configuration.getBoolean(Constants.Security.ENABLED);
    if (securityEnabled) {
      externalAuthenticationServer = injector.getInstance(ExternalAuthenticationServer.class);
    }

    boolean exploreEnabled = configuration.getBoolean(Constants.Explore.EXPLORE_ENABLED);
    if (exploreEnabled) {
      ExploreServiceUtils.checkHiveSupportWithoutSecurity(this.getClass().getClassLoader());
      exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    }

    exploreClient = injector.getInstance(ExploreClient.class);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          shutDown();
        } catch (Throwable e) {
          LOG.error("Failed to shutdown", e);
          // because shutdown hooks execute concurrently, the logger may be closed already: thus also print it.
          System.err.println("Failed to shutdown: " + e.getMessage());
          e.printStackTrace(System.err);
        }
      }
    });
  }

  /**
   * Start the service.
   */
  public void startUp() throws Exception {
    // Start all the services.
    txService.startAndWait();
    metricsCollectionService.startAndWait();
    datasetService.startAndWait();
    serviceStore.startAndWait();

    // It is recommended to initialize log appender after datasetService is started,
    // since log appender instantiates a dataset.
    logAppenderInitializer.initialize();

    Service.State state = appFabricServer.startAndWait();
    if (state != Service.State.RUNNING) {
      throw new Exception("Failed to start Application Fabric");
    }

    metricsQueryService.startAndWait();
    router.startAndWait();
    if (webCloudAppService != null) {
      webCloudAppService.startAndWait();
    }
    streamHttpService.startAndWait();

    if (securityEnabled) {
      externalAuthenticationServer.startAndWait();
    }

    if (exploreExecutorService != null) {
      exploreExecutorService.startAndWait();
    }

    String hostname = InetAddress.getLocalHost().getHostName();
    String protocol = sslEnabled ? "https" : "http";
    int dashboardPort = sslEnabled ?
      configuration.getInt(Constants.Dashboard.SSL_BIND_PORT) :
      configuration.getInt(Constants.Dashboard.BIND_PORT);
    System.out.println("Standalone CDAP started successfully.");
    System.out.printf("Connect to the Console at %s://%s:%d\n", protocol, hostname, dashboardPort);
  }

  /**
   * Shutdown the service.
   */
  public void shutDown() {
    LOG.info("Shutting down Standalone CDAP");

    try {
      // order matters: first shut down web app 'cause it will stop working after router is down
      if (webCloudAppService != null) {
        webCloudAppService.stopAndWait();
      }
      //  shut down router to stop all incoming traffic
      router.stopAndWait();
      // now the stream writer and the explore service (they need tx)
      streamHttpService.stopAndWait();
      if (exploreExecutorService != null) {
        exploreExecutorService.stopAndWait();
      }
      exploreClient.close();
      serviceStore.stopAndWait();
      // app fabric will also stop all programs
      appFabricServer.stopAndWait();
      // all programs are stopped: dataset service, metrics, transactions can stop now
      datasetService.stopAndWait();
      metricsQueryService.stopAndWait();
      txService.stopAndWait();

      if (securityEnabled) {
        // auth service is on the side anyway
        externalAuthenticationServer.stopAndWait();
      }
      logAppenderInitializer.close();

    } catch (Throwable e) {
      LOG.error("Exception during shutdown", e);
      // we can't do much but exit. Because there was an exception, some non-daemon threads may still be running.
      // therefore System.exit() won't do it, we need to farce a halt.
      Runtime.getRuntime().halt(1);
    }
  }

  /**
   * Print the usage statement and return null.
   *
   * @param error indicates whether this was invoked as the result of an error
   * @throws IllegalArgumentException in case of error
   */
  static void usage(boolean error) {

    // Which output stream should we use?
    PrintStream out = (error ? System.err : System.out);

    // And our requirements and usage
    out.println("Requirements: ");
    out.println("  Java:    JDK 1.6+ must be installed and JAVA_HOME environment variable set to the java executable");
    out.println("  Node.js: Node.js must be installed (obtain from http://nodejs.org/#download).  ");
    out.println("           The \"node\" executable must be in the system $PATH environment variable");
    out.println("");
    out.println("Usage: ");
    if (OSDetector.isWindows()) {
      out.println("  cdap.bat [options]");
    } else {
      out.println("  ./cdap.sh [options]");
    }
    out.println("");
    out.println("Additional options:");
    out.println("  --web-app-path  Path to Webapp");
    out.println("  --help          To print this message");
    out.println("");

    if (error) {
      throw new IllegalArgumentException();
    }
  }

  public static void main(String[] args) {
    String webAppPath = WebCloudAppService.WEB_APP;

    if (args.length > 0) {
      if ("--help".equals(args[0]) || "-h".equals(args[0])) {
        usage(false);
        return;
      } else if ("--web-app-path".equals(args[0])) {
        webAppPath = args[1];
      } else {
        usage(true);
      }
    }

    StandaloneMain main = null;

    try {
      main = create(webAppPath);
      main.startUp();
    } catch (Throwable e) {
      System.err.println("Failed to start Standalone CDAP. " + e.getMessage());
      LOG.error("Failed to start Standalone CDAP", e);
      if (main != null) {
        main.shutDown();
      }
      System.exit(-2);
    }
  }

  public static StandaloneMain create() {
    return create(WebCloudAppService.WEB_APP);
  }

  /**
   * The root of all goodness!
   */
  public static StandaloneMain create(String webAppPath) {
    return create(webAppPath, CConfiguration.create(), new Configuration());
  }

  public static StandaloneMain create(CConfiguration cConf, Configuration hConf) {
    return create(WebCloudAppService.WEB_APP, cConf, hConf);
  }

  public static StandaloneMain create(String webAppPath, CConfiguration cConf, Configuration hConf) {
    // This is needed to use LocalJobRunner with fixes (we have it in app-fabric).
    // For the modified local job runner
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();
    // Due to incredibly stupid design of Limits class, once it is initialized, it keeps its settings. We
    // want to make sure it uses our settings in this hConf, so we have to force it initialize here before
    // someone else initializes it.
    Limits.init(hConf);

    File localDataDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR));
    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    hConf.set(Constants.AppFabric.OUTPUT_DIR, cConf.get(Constants.AppFabric.OUTPUT_DIR));
    hConf.set("hadoop.tmp.dir", new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsolutePath());

    // Windows specific requirements
    if (OSDetector.isWindows()) {
      String userDir = System.getProperty("user.dir");
      System.load(userDir + "/lib/native/hadoop.dll");
    }

    //Run dataset service on random port
    List<Module> modules = createPersistentModules(cConf, hConf, webAppPath);

    return new StandaloneMain(modules, cConf, webAppPath);
  }

  private static List<Module> createPersistentModules(CConfiguration configuration, Configuration hConf,
                                                      final String webAppPath) {
    configuration.setIfUnset(Constants.CFG_DATA_LEVELDB_DIR, Constants.DEFAULT_DATA_LEVELDB_DIR);

    String environment =
      configuration.get(Constants.CFG_APPFABRIC_ENVIRONMENT, Constants.DEFAULT_APPFABRIC_ENVIRONMENT);
    if (environment.equals("vpc")) {
      System.err.println("Application Server Environment: " + environment);
    }

    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.LEVELDB.name());

    return ImmutableList.of(
      new AbstractModule() {
        @Override
        protected void configure() {
          if (webAppPath != null) {
            bindConstant().annotatedWith(Names.named("web-app-path")).to(webAppPath);
          }
        }
      },
      new ConfigModule(configuration, hConf),
      new IOModule(),
      new MetricsHandlerModule(),
      new AuthModule(),
      new DiscoveryRuntimeModule().getStandaloneModules(),
      new LocationRuntimeModule().getStandaloneModules(),
      new AppFabricServiceRuntimeModule().getStandaloneModules(),
      new ProgramRunnerRuntimeModule().getStandaloneModules(),
      new DataFabricModules().getStandaloneModules(),
      new DataSetsModules().getLocalModule(),
      new DataSetServiceModules().getLocalModule(),
      new MetricsClientRuntimeModule().getStandaloneModules(),
      new LoggingModules().getStandaloneModules(),
      new RouterModules().getStandaloneModules(),
      new SecurityModules().getStandaloneModules(),
      new StreamServiceRuntimeModule().getStandaloneModules(),
      new ExploreRuntimeModule().getStandaloneModules(),
      new ServiceStoreModules().getStandaloneModule(),
      new ExploreClientModule(),
      new NotificationFeedServiceRuntimeModule().getStandaloneModules(),
      new StreamAdminModules().getStandaloneModules()
    );
  }
}
