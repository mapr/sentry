/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.service.thrift;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * SentryServiceClientPoolFactory is for connection pool to manage the object. Implement the related
 * method to create object, destroy object and wrap object.
 */

public class SentryServiceClientPoolFactory extends BasePooledObjectFactory<SentryPolicyServiceClient> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryServiceClientPoolFactory.class);

  //private final String addr;
  //private final int port;
  //private final Configuration conf;

  public SentryServiceClientPoolFactory(String addr, int port,
                                        Configuration conf) {
    LOGGER.debug("addr = " + addr + "port = " + String.valueOf(port) + " conf = ", conf.toString());
    //this.addr = addr;
    //this.port = port;
    //this.conf = conf;
  }

  @Override
  public SentryPolicyServiceClient create() throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public PooledObject<SentryPolicyServiceClient> wrap(SentryPolicyServiceClient client) {
    return new DefaultPooledObject<SentryPolicyServiceClient>(client);
  }

  @Override
  public void destroyObject(PooledObject<SentryPolicyServiceClient> pooledObject) {
    throw new NotImplementedException();
  }
}
