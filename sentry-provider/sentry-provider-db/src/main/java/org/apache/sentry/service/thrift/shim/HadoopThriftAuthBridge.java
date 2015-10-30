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
package org.apache.sentry.service.thrift.shim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import java.io.IOException;
import java.security.PrivilegedAction;

public abstract class HadoopThriftAuthBridge {

    protected static final String KERBEROS = "KERBEROS";

    public Client createClient() {
        throw new UnsupportedOperationException(
                "The current version of Hadoop does not support Authentication");
    }

    public Server createServer(String keytabFile, String principalConf) throws TTransportException {
        throw new UnsupportedOperationException(
                "The current version of Hadoop does not support Authentication");
    }

    public static abstract class Client {

        public abstract TTransport createClientTransport(String principalConfig, String host, TTransport underlyingTransport, boolean wrapUgi)
                throws IOException;
    }

    public static abstract class Server {
        public abstract TTransportFactory createTransportFactory(Configuration conf) throws TTransportException, IOException;
    }

    public static class TUGIAssumingTransportFactory extends TTransportFactory {
        private final UserGroupInformation ugi;
        private final TTransportFactory wrapped;

        public TUGIAssumingTransportFactory(TTransportFactory wrapped, UserGroupInformation ugi) {
            assert wrapped != null;
            assert ugi != null;

            this.wrapped = wrapped;
            this.ugi = ugi;
        }

        @Override
        public TTransport getTransport(final TTransport trans) {
            return ugi.doAs(new PrivilegedAction<TTransport>() {
                public TTransport run() {
                    return wrapped.getTransport(trans);
                }
            });
        }
    }
}
