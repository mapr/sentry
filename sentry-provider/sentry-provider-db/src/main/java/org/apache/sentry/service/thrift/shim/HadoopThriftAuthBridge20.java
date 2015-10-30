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
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClientDefaultImpl;
import org.apache.sentry.service.thrift.GSSCallback;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.apache.thrift.transport.*;

import java.io.IOException;

public class HadoopThriftAuthBridge20 extends HadoopThriftAuthBridge {

    @Override
    public Client createClient() {
        return new Client();
    }

    @Override
    public Server createServer(String keytabFile, String principalConf) throws TTransportException {
        return new Server(keytabFile, principalConf);
    }

    public static class Client extends HadoopThriftAuthBridge.Client {

        @Override
        public TTransport createClientTransport(String principalConfig, String host, TTransport underlyingTransport, boolean wrapUgi) throws IOException {
            Preconditions.checkNotNull(principalConfig, "Unsupported authentication method. Only Kerberos is supported. " +
                    "To use Kerberos please set value of 'sentry.service.security.mode' property to 'kerberos'");

            String serverPrincipal = SecurityUtil.getServerPrincipal(principalConfig, host);
            String names[] = SaslRpcServer.splitKerberosName(serverPrincipal);

            Preconditions.checkArgument(names.length == 3,
                    "Kerberos principal should have 3 parts: " + serverPrincipal);

            return new SentryPolicyServiceClientDefaultImpl.UgiSaslClientTransport(SaslRpcServer.AuthMethod.KERBEROS.getMechanismName(),
                    null, names[0], names[1],
                    ServiceConstants.ClientConfig.SASL_PROPERTIES, null, underlyingTransport, wrapUgi);
        }
    }

    public static class Server extends HadoopThriftAuthBridge.Server {

        final UserGroupInformation realUgi;

        public Server() throws TTransportException {
            try {
                realUgi = UserGroupInformation.getCurrentUser();
            } catch (IOException ioe) {
                throw new TTransportException(ioe);
            }
        }
        /**
         * Create a server with a kerberos keytab/principal.
         */
        protected Server(String keytabFile, String principalConf)
                throws TTransportException {
            if (keytabFile == null || keytabFile.isEmpty()) {
                throw new TTransportException("No keytab specified");
            }
            if (principalConf == null || principalConf.isEmpty()) {
                throw new TTransportException("No principal specified");
            }

            // Login from the keytab
            String kerberosName;
            try {
                kerberosName =
                        SecurityUtil.getServerPrincipal(principalConf, "0.0.0.0");
                UserGroupInformation.loginUserFromKeytab(
                        kerberosName, keytabFile);
                realUgi = UserGroupInformation.getLoginUser();
                assert realUgi.isFromKeytab();
            } catch (IOException ioe) {
                throw new TTransportException(ioe);
            }
        }

        @Override
        public TTransportFactory createTransportFactory(Configuration conf)
                throws TTransportException, IOException {
            TSaslServerTransport.Factory transFactory = new TSaslServerTransport.Factory();

            String kerberosName = realUgi.getUserName();
            final String names[] = SaslRpcServer.splitKerberosName(kerberosName);
            if (names.length != 3) {
                throw new TTransportException("Kerberos principal should have 3 parts: " + kerberosName);
            }

            transFactory.addServerDefinition(SaslRpcServer.AuthMethod.KERBEROS
                            .getMechanismName(), names[0], names[1],
                    ServiceConstants.ServerConfig.SASL_PROPERTIES, new GSSCallback(conf));

            return new TUGIAssumingTransportFactory(transFactory, realUgi);
        }

    }

}
