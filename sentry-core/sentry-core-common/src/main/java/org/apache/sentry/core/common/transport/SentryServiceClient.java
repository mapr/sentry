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

import java.io.Closeable;

/**
 * Client interface for Proxy Invocation handlers
 * <p>
 * Defines interface that Sentry client's should expose to the Invocation handlers like
 * <code>RetryClientInvocationHandler</code> used to proxy the method invocation on sentry
 * client instances .
 * <p>
 * All the sentry clients that need retrying and failover capabilities should implement
 * this interface.
 */
public interface SentryServiceClient extends Closeable {
  /**
   * This is a no-op when already connected.
   * When there is a connection error, it will retry with another sentry server. It will
   * first cycle through all the available sentry servers, and then retry the whole server
   * list no more than connectionFullRetryTotal times. In this case, it won't introduce
   * more latency when some server fails. Also to prevent all clients connecting to the
   * same server, it will reorder the endpoints randomly after a full retry.
   * <p>
   * TODO: Add metrics for the number of successful connects and errors per client, and total number of retries.
   */
  void connectWithRetry(boolean tryAlternateServer) throws Exception;

  int getRetryCount();

  void close();
}