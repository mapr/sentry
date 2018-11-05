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

package org.apache.sentry.maprminicluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;

public class MapRMiniDFSCluster {
  private Configuration conf;
  private FileSystem fs;

  private Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  public MapRMiniDFSCluster() throws IOException {
    conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
  }

  public MapRMiniDFSCluster(Configuration conf) throws IOException {
    this.conf = conf;
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
  }

  public JobConf createJobConf() {
    if(conf == null){
      JobConf jobConf = new JobConf();
      jobConf.set("fs.default.name", "file:///");
      return jobConf;
    }
    return new JobConf(conf);
  }

  public FileSystem getFileSystem() throws IOException {
    return fs;
  }

  public void shutdown() {

  }

  /**
   * wait for the cluster to get out of safemode.
   */
  public void waitClusterUp() throws IOException {
  }
}
