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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.rpcauth.RpcAuthMethod;
import org.apache.hadoop.security.rpcauth.RpcAuthRegistry;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClientDefaultImpl;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.apache.thrift.transport.*;

import javax.security.sasl.SaslException;
import java.io.IOException;
import java.util.Locale;

public class HadoopThriftAuthBridge25 extends HadoopThriftAuthBridge20 {

    @Override
    public Server createServer(String keytabFile, String principalConf) throws TTransportException {
        if (keytabFile.isEmpty() || principalConf.isEmpty()) {
            return new Server();
        } else {
            return new Server(keytabFile, principalConf);
        }
    }

    @Override
    public Client createClient() {
        return new Client();
    }

    public static class Server extends HadoopThriftAuthBridge20.Server {


        public Server() throws TTransportException {
            super();
        }

        protected Server(String keytabFile, String principalConf)
                throws TTransportException {
            super(keytabFile, principalConf);
        }

        @Override
        public TTransportFactory createTransportFactory(Configuration conf)
                throws TTransportException, IOException {
            TSaslServerTransport.Factory transFactory;
            transFactory = new TSaslServerTransport.Factory();
            RpcAuthMethod rpcAuthMethod = RpcAuthRegistry.getAuthMethod(realUgi.getAuthenticationMethod());

            if (rpcAuthMethod.getAuthenticationMethod().equals(UserGroupInformation.AuthenticationMethod.KERBEROS)) {
                return  super.createTransportFactory(conf);
            } else {
                transFactory.addServerDefinition(rpcAuthMethod.getMechanismName(),
                        null,
                        SaslRpcServer.SASL_DEFAULT_REALM,
                        ServiceConstants.ServerConfig.SASL_PROPERTIES,
                        rpcAuthMethod.createCallbackHandler());
            }

            return new TUGIAssumingTransportFactory(transFactory, realUgi);
        }
    }

    public static class Client extends HadoopThriftAuthBridge20.Client {

        @Override
        public TTransport createClientTransport(String principalConfig, String host, TTransport underlyingTransport, boolean wrapUgi) throws IOException {
            String authTypeStr = System.getProperty(ServiceConstants.ServerConfig.SECURITY_MODE);
            if (authTypeStr == null || authTypeStr.equalsIgnoreCase("other")) {
                authTypeStr = "MAPRSASL";
            }

            RpcAuthMethod rpcAuthMethod = RpcAuthRegistry.getAuthMethod(authTypeStr.toUpperCase(Locale.ENGLISH));
            if (rpcAuthMethod == null) {
                throw new IOException("Unsupported authentication method: " + authTypeStr);
            }

            if (authTypeStr.equalsIgnoreCase("KERBEROS")) {
                return  super.createClientTransport(principalConfig, host, underlyingTransport, wrapUgi);
            } else {
                try {
                    Preconditions.checkArgument(!rpcAuthMethod.getMechanismName().equals(KERBEROS),
                            "Your system is configured to use Kerberos authentication. " +
                                    "You should set value of 'sentry.service.security.mode' property to 'kerberos'");

                    return new SentryPolicyServiceClientDefaultImpl.UgiSaslClientTransport(rpcAuthMethod.getMechanismName(),
                            null, null, null,
                            ServiceConstants.ClientConfig.SASL_PROPERTIES, null, underlyingTransport, wrapUgi);
                } catch (SaslException se) {
                    throw new IOException("Could not instantiate SASL transport", se);
                }
            }
        }
    }
}
