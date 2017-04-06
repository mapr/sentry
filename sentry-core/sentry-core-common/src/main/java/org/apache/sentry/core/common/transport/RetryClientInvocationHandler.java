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
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.exception.SentryHdfsServiceException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * The RetryClientInvocationHandler is a proxy class for handling thrift calls for non-pool
 * model. Currently only one client connection is allowed.
 * <p>
 * For every rpc call, if the client is not connected, it will first connect to one of the
 * sentry servers, and then do the thrift call to the connected sentry server, which will
 * execute the requested method and return back the response. If it is failed with connection
 * problem, it will close the current connection and retry (reconnect and resend the
 * thrift call) no more than rpcRetryTotal times. If the client is already connected, it
 * will reuse the existing connection, and do the thrift call.
 * <p>
 * During reconnection, it will first cycle through all the available sentry servers, and
 * then retry the whole server list no more than connectionFullRetryTotal times. In this
 * case, it won't introduce more latency when some server fails. Also to prevent all
 * clients connecting to the same server, it will reorder the endpoints randomly after a
 * full retry.
 * <p>
 * TODO(kalyan) allow multiple client connections using <code>PoolClientInvocationHandler</code>
 */

public class RetryClientInvocationHandler extends SentryClientInvocationHandler {
  private static final Logger LOGGER =
    LoggerFactory.getLogger(RetryClientInvocationHandler.class);
  private SentryServiceClient client = null;

  /**
   * Initialize the sentry configurations, including rpc retry count and client connection
   * configs for SentryPolicyServiceClientDefaultImpl
   */
  public RetryClientInvocationHandler(Configuration conf, SentryServiceClient clientObject) {
    Preconditions.checkNotNull(conf, "Configuration object cannot be null");
    client = clientObject;
  }

  /**
   * For every rpc call, if the client is not connected, it will first connect to a sentry
   * server, and then do the thrift call to the connected sentry server, which will
   * execute the requested method and return back the response. If it is failed with
   * connection problem, it will close the current connection, and retry (reconnect and
   * resend the thrift call) no more than rpcRetryTotal times. Throw SentryUserException
   * if failed retry after rpcRetryTotal times.
   * if it is failed with other exception, method would just re-throw the exception.
   * Synchronized it for thread safety.
   */
  @Override
  synchronized public Object invokeImpl(Object proxy, Method method, Object[] args) throws Exception {
    int retryCount = 0;
    Exception lastExc = null;
    boolean tryAlternateServer = false;

    while (retryCount < client.getRetryCount()) {
      // Connect to a sentry server if not connected yet.
      try {
        client.connectWithRetry(tryAlternateServer);
      } catch (IOException e) {
        // Increase the retry num
        // Retry when the exception is caused by connection problem.
        retryCount++;
        lastExc = e;
        close();
        continue;
      }

      // do the thrift call
      try {
        return method.invoke(client, args);
      } catch (InvocationTargetException e) {
        // Get the target exception, check if SentryUserException or TTransportException is wrapped.
        // TTransportException means there has connection problem with the pool.
        Throwable targetException = e.getCause();
        if (targetException instanceof SentryUserException ||
          targetException instanceof SentryHdfsServiceException) {
          Throwable sentryTargetException = targetException.getCause();
          // If there has connection problem, eg, invalid connection if the service restarted,
          // sentryTargetException instanceof TTransportException will be true.
          if (sentryTargetException instanceof TTransportException) {
            // Retry when the exception is caused by connection problem.
            lastExc = new TTransportException(sentryTargetException);
            LOGGER.error("Got TTransportException when do the thrift call ", lastExc);
            tryAlternateServer = true;
            // Closing the thrift client on TTransportException. New client object is
            // created using new socket when an attempt to reconnect is made.
            close();
          } else {
            // The exception is thrown by thrift call, eg, SentryAccessDeniedException.
            // Do not need to reconnect to the sentry server.
            if (targetException instanceof SentryUserException) {
              throw (SentryUserException) targetException;
            } else {
              throw (SentryHdfsServiceException) targetException;
            }
          }
        } else {
          throw e;
        }
      }

      // Increase the retry num
      retryCount++;
    }

    // Throw the exception as reaching the max rpc retry num.
    LOGGER.error(String.format("failed after %d retries ", client.getRetryCount()), lastExc);
    throw new SentryUserException(
      String.format("failed after %d retries ", client.getRetryCount()), lastExc);
  }

  @Override
  public void close() {
    try {
      LOGGER.debug("Closing the current client connection");
      client.close();
    } catch (Exception e) {
      LOGGER.error("Encountered failure while closing the connection");
    }
  }
}
