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

package co.cask.cdap.explore.security;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data.security.HBaseTokenUtils;
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClientCache;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetDelegationTokenRequestPBImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.yarn.YarnUtils;
import org.apache.twill.yarn.YarnSecureStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * A {@link SecureStoreUpdater} that provides update to Hive secure token.
 */
public class HiveSecureStoreUpdater implements SecureStoreUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(HiveSecureStoreUpdater.class);

  private final Configuration conf;
  private final LocationFactory locationFactory;
  private long nextUpdateTime = -1;
  private Credentials credentials;

  @Inject
  public HiveSecureStoreUpdater(Configuration conf, LocationFactory locationFactory) {
    this.conf = conf;
    this.locationFactory = locationFactory;
    this.credentials = new Credentials();
  }

  private void printToken(String type, Token token) {
    LOG.info("!!{} token: {}, {}, {}",
             type, token.getService(), Bytes.toString(token.getIdentifier()), token.getKind());
  }

  private void refreshCredentials() {
    try {
      Credentials creds = new Credentials();
      HBaseTokenUtils.obtainToken(conf, creds);
      HiveTokenUtils.obtainToken(creds);

      LOG.info("Obtaining delegation token for Yarn RM");
      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(conf);
      yarnClient.start();
      org.apache.hadoop.security.token.Token<TokenIdentifier> rmToken =
        ConverterUtils.convertFromYarn(yarnClient.getRMDelegationToken(
          new Text(UserGroupInformation.getCurrentUser().getShortUserName())), YarnUtils.getRMAddress(conf));

      yarnClient.stop();

      printToken("RMToken", rmToken);


      creds.addToken(new Text(rmToken.getService()), rmToken);

      MRClientCache clientCache = new MRClientCache(conf, new ResourceMgrDelegate(new YarnConfiguration(conf)));
      MRClientProtocol hsProxy = clientCache.getInitializedHSProxy();
      GetDelegationTokenRequest request = new GetDelegationTokenRequestPBImpl();
//      request.setRenewer(UserGroupInformation.getCurrentUser().getShortUserName());
      request.setRenewer("yarn");

      LOG.info("!!! Credentials before");
      for (Token t : creds.getAllTokens()) {
        LOG.info("{}", t);
      }

      try {
        String historyServerAddress = conf.get("mapreduce.jobhistory.address");
        LOG.info("HostAndPort for HistoryServer: {}", historyServerAddress);
        HostAndPort hostAndPort = HostAndPort.fromString(historyServerAddress);
        org.apache.hadoop.security.token.Token<TokenIdentifier> hsToken =
          ConverterUtils.convertFromYarn(hsProxy.getDelegationToken(request).getDelegationToken(),
                                         new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort())
        );

        printToken("MRToken", hsToken);

        LOG.info("HSToken Name: {}", hsToken.getService());
        creds.addToken(new Text(hsToken.getService()), hsToken);

      } catch (Throwable t) {
        LOG.error("!!! Got error for HS:", t);
      }

      LOG.info("!!! Credentials after");
      for (Token t : creds.getAllTokens()) {
        LOG.info("{}", t);
      }

      YarnUtils.addDelegationTokens(conf, locationFactory, creds);
      credentials = creds;
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    } catch (YarnException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Returns the update interval for the Hive delegation token.
   * @return The update interval in milliseconds.
   */
  public long getUpdateInterval() {
    // TODO change that using hive settings - this has just been copied from the HBaseSecureStoreUpdater

    // The value contains in hbase-default.xml, so it should always there. If it is really missing, default it to 1 day.
//    return conf.getLong(Constants.HBase.AUTH_KEY_UPDATE_INTERVAL, TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS));
    return TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
  }

  @Override
  public SecureStore update(String application, RunId runId) {
    long now = System.currentTimeMillis();
    if (now >= nextUpdateTime) {
      nextUpdateTime = now + getUpdateInterval();
      refreshCredentials();
    }
    return YarnSecureStore.create(credentials);
  }

  private static class MRClientCache extends ClientCache {

    public MRClientCache(Configuration conf, ResourceMgrDelegate rm) {
      super(conf, rm);
    }

    @Override
    public synchronized MRClientProtocol getInitializedHSProxy() throws IOException {
      return super.getInitializedHSProxy();
    }

  }
}
