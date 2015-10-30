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

import org.apache.hadoop.util.VersionInfo;

import java.util.HashMap;
import java.util.Map;

public abstract class ShimLoader {
    private static HadoopThriftAuthBridge hadoopThriftAuthBridge;

    /**
     * The names of the clasЫses for shimming {@link HadoopThriftAuthBridge}
     */
    private static final HashMap<String, String> HADOOP_THRIFT_AUTH_BRIDGE_CLASSES =
            new HashMap<String, String>();

    static {
        HADOOP_THRIFT_AUTH_BRIDGE_CLASSES.put("0.20",
                "org.apache.sentry.service.thrift.shim.HadoopThriftAuthBridge20");
        HADOOP_THRIFT_AUTH_BRIDGE_CLASSES.put("2.5",
                "org.apache.sentry.service.thrift.shim.HadoopThriftAuthBridge25");
    }

    public static synchronized HadoopThriftAuthBridge getHadoopThriftAuthBridge() {
        if (hadoopThriftAuthBridge == null) {
            hadoopThriftAuthBridge = loadShims(HADOOP_THRIFT_AUTH_BRIDGE_CLASSES,
                    HadoopThriftAuthBridge.class);
        }
        return hadoopThriftAuthBridge;
    }

    private static <T> T loadShims(Map<String, String> classMap, Class<T> xface) {
        String vers = getMajorVersion();
        String className = classMap.get(vers);
        return createShim(className, xface);
    }

    public static String getMajorVersion() {
        String vers = VersionInfo.getVersion();

        String[] parts = vers.split("\\.");
        if (parts.length < 2) {
            throw new RuntimeException("Illegal Hadoop Version: " + vers +
                    " (expected A.B.* format)");
        }

        // Special handling for Hadoop 1.x and 2.x
        switch (Integer.parseInt(parts[0])) {
            case 0:
                throw new IllegalArgumentException("Unrecognized Hadoop major version number: " + vers);
            case 1:
                return "0.20";
            case 2:
                int minor = Integer.parseInt(parts[1]);
                if (minor < 5) {
                    return "0.20";
                } else {
                    return "2.5";
                }
            default:
                throw new IllegalArgumentException("Unrecognized Hadoop major version number: " + vers);
        }
    }

    private static <T> T createShim(String className, Class<T> xface) {
        try {
            Class<?> clazz = Class.forName(className);
            return xface.cast(clazz.newInstance());
        } catch (Exception e) {
            throw new RuntimeException("Could not load shims in class " +
                    className, e);
        }
    }
}
