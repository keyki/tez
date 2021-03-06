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

package org.apache.tez.dag.app;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.app.MockDAGAppMaster.MockContainerLauncher;
import org.apache.tez.dag.app.MockDAGAppMaster.MockContainerLauncher.ContainerData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

@SuppressWarnings("deprecation")
public class TestMockDAGAppMaster {
  static Configuration defaultConf;
  static FileSystem localFs;
  static Path workDir;
  
  static {
    try {
      defaultConf = new Configuration(false);
      defaultConf.set("fs.defaultFS", "file:///");
      defaultConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
      localFs = FileSystem.getLocal(defaultConf);
      workDir = new Path(new Path(System.getProperty("test.build.data", "/tmp")),
          "TestDAGAppMaster").makeQualified(localFs);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }
  
  @Test (timeout = 5000)
  public void testLocalResourceSetup() throws Exception {
    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    
    MockTezClient tezClient = new MockTezClient("testMockAM", tezconf, true, null, null, null);
    tezClient.start();
    
    MockDAGAppMaster mockApp = tezClient.getLocalClient().getMockApp();
    MockContainerLauncher mockLauncher = mockApp.getContainerLauncher();
    mockLauncher.startScheduling(false);
    
    Map<String, LocalResource> lrDAG = Maps.newHashMap();
    String lrName1 = "LR1";
    lrDAG.put(lrName1, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    Map<String, LocalResource> lrVertex = Maps.newHashMap();
    String lrName2 = "LR2";
    lrVertex.put(lrName2, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test1"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));

    DAG dag = DAG.create("test").addTaskLocalFiles(lrDAG);
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), 5).addTaskLocalFiles(lrVertex);
    dag.addVertex(vA);

    DAGClient dagClient = tezClient.submitDAG(dag);
    mockLauncher.waitTillContainersLaunched();
    ContainerData cData = mockLauncher.getContainers().values().iterator().next();
    ContainerLaunchContext launchContext = cData.launchContext;
    Map<String, LocalResource> taskLR = launchContext.getLocalResources();
    // verify tasks are launched with both DAG and task resources.
    Assert.assertTrue(taskLR.containsKey(lrName1));
    Assert.assertTrue(taskLR.containsKey(lrName2));
    
    mockLauncher.startScheduling(true);
    dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    tezClient.stop();
  }
  
  @Test (timeout = 10000)
  public void testMultipleSubmissions() throws Exception {
    Map<String, LocalResource> lrDAG = Maps.newHashMap();
    String lrName1 = "LR1";
    lrDAG.put(lrName1, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    Map<String, LocalResource> lrVertex = Maps.newHashMap();
    String lrName2 = "LR2";
    lrVertex.put(lrName2, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test1"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));

    DAG dag = DAG.create("test").addTaskLocalFiles(lrDAG);
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), 5).addTaskLocalFiles(lrVertex);
    dag.addVertex(vA);

    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    
    MockTezClient tezClient = new MockTezClient("testMockAM", tezconf, true, null, null, null);
    tezClient.start();
    DAGClient dagClient = tezClient.submitDAG(dag);
    dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    tezClient.stop();
    
    // submit the same DAG again to verify it can be done.
    tezClient = new MockTezClient("testMockAM", tezconf, true, null, null, null);
    tezClient.start();
    dagClient = tezClient.submitDAG(dag);
    dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    tezClient.stop();

  }
  

}
