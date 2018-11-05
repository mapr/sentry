/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sentry.policy.sqoop;

import java.io.File;
import java.io.IOException;

import org.apache.sentry.maprminicluster.MapRMiniDFSCluster;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.sentry.provider.file.PolicyFiles;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestSqoopPolicyEngineDFS extends AbstractTestSqoopPolicyEngine {
  private static MapRMiniDFSCluster dfsCluster;
  private static FileSystem fileSystem;
  private static Path etc;

  @BeforeClass
  public static void setupLocalClazz() throws IOException {
    File baseDir = getBaseDir();
    Assert.assertNotNull(baseDir);
    File dfsDir = new File(baseDir, "dfs");
    Assert.assertTrue(dfsDir.isDirectory() || dfsDir.mkdirs());
    dfsCluster = new MapRMiniDFSCluster(conf);
    fileSystem = dfsCluster.getFileSystem();
    etc = new Path(fileSystem.getWorkingDirectory(), "etc");
    fileSystem.mkdirs(etc);
  }

  @AfterClass
  public static void teardownLocalClazz() {
    if(dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  @Override
  protected void  afterSetup() throws IOException {
    fileSystem.delete(etc, true);
    fileSystem.mkdirs(etc);
    PolicyFiles.copyToDir(fileSystem, etc, "test-authz-provider.ini");
    setPolicy(new SqoopPolicyFileProviderBackend(sqoopServerName, new Path(etc,
        "test-authz-provider.ini").toString(), conf));
  }

  @Override
  protected void beforeTeardown() throws IOException {
    fileSystem.delete(etc, true);
  }
}
