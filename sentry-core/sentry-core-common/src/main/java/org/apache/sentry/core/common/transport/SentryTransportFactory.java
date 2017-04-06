/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.core.common.transport;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.core.common.exception.MissingConfigurationException;
import org.apache.sentry.core.common.utils.ThriftUtil;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Generate Thrift transports suitable for talking to Sentry
 */
public final class SentryTransportFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryTransportFactory.class);

  private final Configuration conf;
  private final SentryClientTransportConfigInterface transportConfig;
  private final ArrayList<InetSocketAddress> endpoints;

  public SentryTransportFactory(Configuration conf,
                                SentryClientTransportConfigInterface configInterface) {
    this.conf = conf;
    this.transportConfig = configInterface;
    String hostsAndPortsStr = transportConfig.getSentryServerRpcAddress(conf);
    int serverPort = transportConfig.getServerRpcPort(conf);

    String[] hostsAndPortsStrArr = hostsAndPortsStr.split(",");
    HostAndPort[] hostsAndPorts = ThriftUtil.parseHostPortStrings(hostsAndPortsStrArr, serverPort);
    this.endpoints = new ArrayList<>(hostsAndPortsStrArr.length);
    for (HostAndPort endpoint : hostsAndPorts) {
      this.endpoints.add(
              new InetSocketAddress(endpoint.getHostText(), endpoint.getPort()));
      LOGGER.debug("Added server endpoint: " + endpoint.toString());
    }
    // Reorder endpoints randomly to prevent all clients connecting to the same endpoint
    // at the same time after a node failure.
    if (endpoints.size() > 1) {
      Collections.shuffle(endpoints);
    }
  }

  /**
   * This transport wraps the Sasl transports to set up the right UGI context for open().
   */
  private static final class UgiSaslClientTransport extends TSaslClientTransport {
    private static final ImmutableMap<String, String> SASL_PROPERTIES =
            ImmutableMap.of(Sasl.SERVER_AUTH, "true", Sasl.QOP, "auth-conf");

    private UserGroupInformation ugi = null;

    private UgiSaslClientTransport(String mechanism, String protocol,
                                   String serverName, TTransport transport,
                                   boolean wrapUgi, Configuration conf)
            throws IOException, SaslException {
      super(mechanism, null, protocol, serverName, SASL_PROPERTIES, null,
              transport);
      if (wrapUgi) {
        // If we don't set the configuration, the UGI will be created based on
        // what's on the classpath, which may lack the kerberos changes we require
        UserGroupInformation.setConfiguration(conf);
        ugi = UserGroupInformation.getLoginUser();
      }
    }

    // open the SASL transport with using the current UserGroupInformation
    // This is needed to get the current login context stored
    @Override
    public void open() throws TTransportException {
      if (ugi == null) {
        baseOpen();
      } else {
        try {
          if (ugi.isFromKeytab()) {
            ugi.checkTGTAndReloginFromKeytab();
          }
          ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws TTransportException {
              baseOpen();
              return null;
            }
          });
        } catch (IOException e) {
          throw new TTransportException("Failed to open SASL transport: " + e.getMessage(), e);
        } catch (InterruptedException e) {
          throw new TTransportException(
                  "Interrupted while opening underlying transport: " + e.getMessage(), e);
        }
      }
    }

    private void baseOpen() throws TTransportException {
      super.open();
    }
  }

  /**
   * On connection error, Iterates through all the configured servers and tries to connect.
   * On successful connection, control returns
   * On connection failure, continues iterating through all the configured sentry servers,
   * and then retries the whole server list no more than connectionFullRetryTotal times.
   * In this case, it won't introduce more latency when some server fails.
   * <p>
   * TODO: Add metrics for the number of successful connects and errors per client, and total number of retries.
   */
  public TTransport connect() throws IOException {
    int connectionFullRetryTotal = transportConfig.getSentryFullRetryTotal(conf);
    IOException currentException = null;
    for (int retryCount = 0; retryCount < connectionFullRetryTotal; retryCount++) {
      try {
        return connectToAvailableServer();
      } catch (IOException e) {
        currentException = e;
        LOGGER.error(
                String.format("Failed to connect to all the configured sentry servers, " +
                        "Retrying again"));
      }
    }
    // Throw exception as reaching the max full connectWithRetry number.
    LOGGER.error(
            String.format("Reach the max connection retry num %d ", connectionFullRetryTotal),
            currentException);
      throw currentException;
  }

  /**
   * Iterates through all the configured servers and tries to connect.
   * On connection error, tries to connect to next server.
   * Control returns on successful connection OR it's done trying to all the
   * configured servers.
   *
   * @throws IOException
   */
  private TTransport connectToAvailableServer() throws IOException {
    IOException currentException = null;
    for (InetSocketAddress addr : endpoints) {
      try {
        return connect(addr);
      } catch (IOException e) {
        LOGGER.error(String.format("Failed connection to %s: %s",
                addr.toString(), e.getMessage()), e);
        currentException = e;
      }
    }
    if (currentException != null) {
      throw currentException;
    }
    return null;
  }

  /**
   * Connect to the specified socket address and throw IOException if failed.
   *
   * @param serverAddress Address client needs to connect
   * @throws Exception if there is failure in establishing the connection.
   */
  protected TTransport connect(InetSocketAddress serverAddress) throws IOException {
    try {
      TTransport transport = createTransport(serverAddress);
      transport.open();
      LOGGER.info(String.format("Connected to SentryServer: %s", serverAddress));
      return transport;
    } catch (TTransportException e) {
      throw new IOException("Failed to open transport: " + e.getMessage(), e);
    } catch (MissingConfigurationException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /**
   * New socket is is created
   *
   * @param serverAddress
   * @return
   * @throws TTransportException
   * @throws MissingConfigurationException
   * @throws IOException
   */
  private TTransport createTransport(InetSocketAddress serverAddress)
          throws TTransportException, MissingConfigurationException, IOException {
    TTransport socket = new TSocket(serverAddress.getHostName(),
            serverAddress.getPort(), transportConfig.getServerRpcConnTimeoutInMs(conf));

    if (!transportConfig.isKerberosEnabled(conf)) {
      return socket;
    }

    String serverPrincipal = transportConfig.getSentryPrincipal(conf);
    serverPrincipal = SecurityUtil.getServerPrincipal(serverPrincipal, serverAddress.getAddress());
    LOGGER.debug("Using server kerberos principal: " + serverPrincipal);
    String[] serverPrincipalParts = SaslRpcServer.splitKerberosName(serverPrincipal);
    Preconditions.checkArgument(serverPrincipalParts.length == 3,
            "Kerberos principal should have 3 parts: " + serverPrincipal);

    boolean wrapUgi = transportConfig.useUserGroupInformation(conf);
    return new UgiSaslClientTransport(SaslRpcServer.AuthMethod.KERBEROS.getMechanismName(),
            serverPrincipalParts[0], serverPrincipalParts[1],
            socket, wrapUgi, conf);
  }
}
