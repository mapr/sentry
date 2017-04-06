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
package org.apache.sentry.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryHdfsServiceException;
import org.apache.sentry.core.common.transport.SentryServiceClientTransportDefaultImpl;
import org.apache.sentry.hdfs.service.thrift.SentryHDFSService;
import org.apache.sentry.hdfs.service.thrift.SentryHDFSService.Client;
import org.apache.sentry.hdfs.service.thrift.TAuthzUpdateResponse;
import org.apache.sentry.hdfs.service.thrift.TPathsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPermissionsUpdate;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 Sentry HDFS Service Client

 The public implementation of SentryHDFSServiceClient.
 A Sentry Client in which all the operations are synchronized for thread safety
 Note: When using this client, if there is an exception in RPC, socket can get into an inconsistent state.
 So it is important to close and re-open the transport so that new socket is used.
*/


public class SentryHDFSServiceClientDefaultImpl extends SentryServiceClientTransportDefaultImpl implements SentryHDFSServiceClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryHDFSServiceClientDefaultImpl.class);

  private Client client;

  public SentryHDFSServiceClientDefaultImpl(Configuration conf) throws IOException {
    super(conf, sentryClientType.HDFS_CLIENT);
  }

  /**
   * Connect to the specified socket address and then use the new socket
   * to construct new thrift client.
   *
   * @param serverAddress: socket address to which the client should connect.
   * @throws IOException
   */
  public void connect(InetSocketAddress serverAddress) throws IOException {
    TProtocol tProtocol = null;
    super.connect(serverAddress);
    long maxMessageSize = conf.getLong(ServiceConstants.ClientConfig.SENTRY_HDFS_THRIFT_MAX_MESSAGE_SIZE,
            ServiceConstants.ClientConfig.SENTRY_HDFS_THRIFT_MAX_MESSAGE_SIZE_DEFAULT);
    if (conf.getBoolean(ServiceConstants.ClientConfig.USE_COMPACT_TRANSPORT,
            ServiceConstants.ClientConfig.USE_COMPACT_TRANSPORT_DEFAULT)) {
      tProtocol = new TCompactProtocol(transport, maxMessageSize, maxMessageSize);
    } else {
      tProtocol = new TBinaryProtocol(transport, maxMessageSize, maxMessageSize, true, true);
    }
    TMultiplexedProtocol protocol = new TMultiplexedProtocol(
            tProtocol, SentryHDFSServiceClient.SENTRY_HDFS_SERVICE_NAME);
    client = new SentryHDFSService.Client(protocol);
    LOGGER.info("Successfully created client");
  }

  public synchronized void notifyHMSUpdate(PathsUpdate update)
          throws SentryHdfsServiceException {
    try {
      client.handle_hms_notification(update.toThrift());
    } catch (Exception e) {
      throw new SentryHdfsServiceException("Thrift Exception occurred !!", e);
    }
  }

  public synchronized long getLastSeenHMSPathSeqNum()
          throws SentryHdfsServiceException {
    try {
      return client.check_hms_seq_num(-1);
    } catch (Exception e) {
      throw new SentryHdfsServiceException("Thrift Exception occurred !!", e);
    }
  }

  public synchronized SentryAuthzUpdate getAllUpdatesFrom(long permSeqNum, long pathSeqNum)
          throws SentryHdfsServiceException {
    SentryAuthzUpdate retVal = new SentryAuthzUpdate(new LinkedList<PermissionsUpdate>(), new LinkedList<PathsUpdate>());
    try {
      TAuthzUpdateResponse sentryUpdates = client.get_all_authz_updates_from(permSeqNum, pathSeqNum);
      if (sentryUpdates.getAuthzPathUpdate() != null) {
        for (TPathsUpdate pathsUpdate : sentryUpdates.getAuthzPathUpdate()) {
          retVal.getPathUpdates().add(new PathsUpdate(pathsUpdate));
        }
      }
      if (sentryUpdates.getAuthzPermUpdate() != null) {
        for (TPermissionsUpdate permsUpdate : sentryUpdates.getAuthzPermUpdate()) {
          retVal.getPermUpdates().add(new PermissionsUpdate(permsUpdate));
        }
      }
    } catch (Exception e) {
      throw new SentryHdfsServiceException("Thrift Exception occurred !!", e);
    }
    return retVal;
  }
}
