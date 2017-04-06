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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.core.common.exception.MissingConfigurationException;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.sentry.core.common.utils.ThriftUtil;
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
 * Implements the transport functionality for sentry clients.
 * All the sentry clients should extend this class for transport implementation.
 */

public abstract class SentryServiceClientTransportDefaultImpl {
  protected final Configuration conf;
  protected final boolean kerberos;
  private String[] serverPrincipalParts;

  protected TTransport transport;
  private final int connectionTimeout;
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryServiceClientTransportDefaultImpl.class);
  // configs for connection retry
  private final int connectionFullRetryTotal;
  private final int rpcRetryTotal;
  private final ArrayList<InetSocketAddress> endpoints;
  protected InetSocketAddress serverAddress;
  private final SentryClientTransportConfigInterface transportConfig;
  private static final ImmutableMap<String, String> SASL_PROPERTIES =
    ImmutableMap.of(Sasl.SERVER_AUTH, "true", Sasl.QOP, "auth-conf");

  /**
   * Defines various client types.
   */
  protected enum sentryClientType {
    POLICY_CLIENT,
    HDFS_CLIENT,
  }

  /**
   * This transport wraps the Sasl transports to set up the right UGI context for open().
   */
  public static class UgiSaslClientTransport extends TSaslClientTransport {
    UserGroupInformation ugi = null;

    public UgiSaslClientTransport(String mechanism, String protocol,
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
   * Initialize the object based on the sentry configuration provided.
   * List of configured servers are reordered randomly preventing all
   * clients connecting to the same server.
   *
   * @param conf Sentry configuration
   * @param type Type indicates the service type
   */
  public SentryServiceClientTransportDefaultImpl(Configuration conf,
                                                 sentryClientType type) throws IOException {

    this.conf = conf;
    Preconditions.checkNotNull(this.conf, "Configuration object cannot be null");
    serverPrincipalParts = null;
    if (type == sentryClientType.POLICY_CLIENT) {
      transportConfig = new SentryPolicyClientTransportConfig();
    } else {
      transportConfig = new SentryHDFSClientTransportConfig();
    }

    try {
      String hostsAndPortsStr;
      this.connectionTimeout = transportConfig.getServerRpcConnTimeoutInMs(conf);
      this.connectionFullRetryTotal = transportConfig.getSentryFullRetryTotal(conf);
      this.rpcRetryTotal = transportConfig.getSentryRpcRetryTotal(conf);
      this.kerberos = transportConfig.isKerberosEnabled(conf);

      hostsAndPortsStr = transportConfig.getSentryServerRpcAddress(conf);

      int serverPort = transportConfig.getServerRpcPort(conf);

      String[] hostsAndPortsStrArr = hostsAndPortsStr.split(",");
      HostAndPort[] hostsAndPorts = ThriftUtil.parseHostPortStrings(hostsAndPortsStrArr, serverPort);

      this.endpoints = new ArrayList(hostsAndPortsStrArr.length);
      for (HostAndPort endpoint : hostsAndPorts) {
        this.endpoints.add(
          new InetSocketAddress(endpoint.getHostText(), endpoint.getPort()));
        LOGGER.debug("Added server endpoint: " + endpoint.toString());
      }

      // Reorder endpoints randomly to prevent all clients connecting to the same endpoint
      // at the same time after a node failure.
      Collections.shuffle(endpoints);
      serverAddress = null;
      connectWithRetry(false);
    } catch (Exception e) {
      throw new RuntimeException("Client Creation Failed: " + e.getMessage(), e);
    }
  }

  /**
   * Initialize object based on the parameters provided provided.
   *
   * @param addr Host address which the client needs to connect
   * @param port Host Port which the client needs to connect
   * @param conf Sentry configuration
   * @param type Type indicates the service type
   */
  public SentryServiceClientTransportDefaultImpl(String addr, int port, Configuration conf,
                                                 sentryClientType type) throws IOException {
    // copy the configuration because we may make modifications to it.
    this.conf = new Configuration(conf);
    serverPrincipalParts = null;
    Preconditions.checkNotNull(this.conf, "Configuration object cannot be null");
    if (type == sentryClientType.POLICY_CLIENT) {
      transportConfig = new SentryPolicyClientTransportConfig();
    } else {
      transportConfig = new SentryHDFSClientTransportConfig();
    }

    try {
      InetSocketAddress serverAddress = NetUtils.createSocketAddr(addr, port);
      this.connectionTimeout = transportConfig.getServerRpcConnTimeoutInMs(conf);
      this.rpcRetryTotal = transportConfig.getSentryRpcRetryTotal(conf);
      this.connectionFullRetryTotal = transportConfig.getSentryFullRetryTotal(conf);
      this.kerberos = transportConfig.isKerberosEnabled(conf);
      connect(serverAddress);
    } catch (MissingConfigurationException e) {
      throw new RuntimeException("Client Creation Failed: " + e.getMessage(), e);
    }
    endpoints = null;
  }


  /**
   * no-op when already connected.
   * On connection error, Iterates through all the configured servers and tries to connect.
   * On successful connection, control returns
   * On connection failure, continues iterating through all the configured sentry servers,
   * and then retries the whole server list no more than connectionFullRetryTotal times.
   * In this case, it won't introduce more latency when some server fails.
   * <p>
   * TODO: Add metrics for the number of successful connects and errors per client, and total number of retries.
   */
  public synchronized void connectWithRetry(boolean tryAlternateServer) throws IOException {
    if (isConnected() && (!tryAlternateServer)) {
      return;
    }

    IOException currentException = null;
    for (int retryCount = 0; retryCount < connectionFullRetryTotal; retryCount++) {
      try {
        connectToAvailableServer();
        return;
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
  private void connectToAvailableServer() throws IOException {
    IOException currentException = null;
    if (endpoints.size() == 1) {
      connect(endpoints.get(0));
      return;
    }

    for (InetSocketAddress addr : endpoints) {
      try {
        serverAddress = addr;
        connect(serverAddress);
        LOGGER.info(String.format("Connected to SentryServer: %s", addr.toString()));
        return;
      } catch (IOException e) {
        LOGGER.error(String.format("Failed connection to %s: %s",
          addr.toString(), e.getMessage()), e);
        currentException = e;
      }
    }
    throw currentException;
  }

  /**
   * Connect to the specified socket address and throw IOException if failed.
   *
   * @param serverAddress Address client needs to connect
   * @throws Exception if there is failure in establishing the connection.
   */
  protected void connect(InetSocketAddress serverAddress) throws IOException {
    try {
      transport = createTransport(serverAddress);
      transport.open();
    } catch (TTransportException e) {
      throw new IOException("Failed to open transport: " + e.getMessage(), e);
    } catch (MissingConfigurationException e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    LOGGER.debug("Successfully opened transport: " + transport + " to " + serverAddress);
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
      serverAddress.getPort(), connectionTimeout);

    if (kerberos) {
      String serverPrincipal = transportConfig.getSentryPrincipal(conf);
      serverPrincipal = SecurityUtil.getServerPrincipal(serverPrincipal, serverAddress.getAddress());
      LOGGER.debug("Using server kerberos principal: " + serverPrincipal);
      if (serverPrincipalParts == null) {
        serverPrincipalParts = SaslRpcServer.splitKerberosName(serverPrincipal);
        Preconditions.checkArgument(serverPrincipalParts.length == 3,
          "Kerberos principal should have 3 parts: " + serverPrincipal);
      }

      boolean wrapUgi = transportConfig.useUserGroupInformation(conf);
      return new UgiSaslClientTransport(SaslRpcServer.AuthMethod.KERBEROS.getMechanismName(),
        serverPrincipalParts[0], serverPrincipalParts[1],
        socket, wrapUgi, conf);
    } else {
      return socket;
    }
  }

  private boolean isConnected() {
    return transport != null && transport.isOpen();
  }

  public synchronized void close() {
    if (isConnected()) {
      transport.close();
    }
  }

  public int getRetryCount() {
    return rpcRetryTotal;
  }
}