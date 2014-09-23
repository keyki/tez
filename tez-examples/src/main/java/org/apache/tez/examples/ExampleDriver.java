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

package org.apache.tez.examples;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.EnumSet;
import java.util.Set;

import org.apache.hadoop.util.ProgramDriver;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;

/**
 * A description of an example program based on its class and a
 * human-readable description.
 */
public class ExampleDriver {

  private static final DecimalFormat formatter = new DecimalFormat("###.##%");

  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("wordcount", WordCount.class,
          "A native Tez wordcount program that counts the words in the input files.");
      pgd.addClass("orderedwordcount", OrderedWordCount.class,
          "Word Count with words sorted on frequency");
      pgd.addClass("simplesessionexample", SimpleSessionExample.class,
          "Example to run multiple dags in a session");
      pgd.addClass("hashjoin", HashJoinExample.class,
          "Identify all occurences of lines in file1 which also occur in file2 using hash join");
      pgd.addClass("sortmergejoin", SortMergeJoinExample.class,
          "Identify all occurences of lines in file1 which also occur in file2 using sort merge join");
      pgd.addClass("joindatagen", JoinDataGen.class,
          "Generate data to run the joinexample");
      pgd.addClass("joinvalidate", JoinValidate.class,
          "Validate data generated by joinexample and joindatagen");
      pgd.addClass("topkdatagen", TopKDataGen.class, "Generate a CSV file");
      pgd.addClass("topk", TopK.class, "Take the top K elements of a CSV file");
      exitCode = pgd.run(argv);
    } catch(Throwable e){
      e.printStackTrace();
    }

    System.exit(exitCode);
  }

  public static void printDAGStatus(DAGClient dagClient, String[] vertexNames)
      throws IOException, TezException {
    printDAGStatus(dagClient, vertexNames, false, false);
  }

  public static void printDAGStatus(DAGClient dagClient, String[] vertexNames,
      boolean displayDAGCounters, boolean displayVertexCounters)
      throws IOException, TezException {
    Set<StatusGetOpts> opts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
    DAGStatus dagStatus = dagClient.getDAGStatus(
      (displayDAGCounters ? opts : null));
    Progress progress = dagStatus.getDAGProgress();
    double vProgressFloat = 0.0f;
    if (progress != null) {
      System.out.println("");
      System.out.println("DAG: State: "
          + dagStatus.getState()
          + " Progress: "
          + (progress.getTotalTaskCount() < 0 ? formatter.format(0.0f) :
            formatter.format((double)(progress.getSucceededTaskCount())
              /progress.getTotalTaskCount())));
      for (String vertexName : vertexNames) {
        VertexStatus vStatus = dagClient.getVertexStatus(vertexName,
          (displayVertexCounters ? opts : null));
        if (vStatus == null) {
          System.out.println("Could not retrieve status for vertex: "
            + vertexName);
          continue;
        }
        Progress vProgress = vStatus.getProgress();
        if (vProgress != null) {
          vProgressFloat = 0.0f;
          if (vProgress.getTotalTaskCount() == 0) {
            vProgressFloat = 1.0f;
          } else if (vProgress.getTotalTaskCount() > 0) {
            vProgressFloat = (double)vProgress.getSucceededTaskCount()
              /vProgress.getTotalTaskCount();
          }
          System.out.println("VertexStatus:"
              + " VertexName: "
              + (vertexName.equals("ivertex1") ? "intermediate-reducer"
                  : vertexName)
              + " Progress: " + formatter.format(vProgressFloat));
        }
        if (displayVertexCounters) {
          TezCounters counters = vStatus.getVertexCounters();
          if (counters != null) {
            System.out.println("Vertex Counters for " + vertexName + ": "
              + counters);
          }
        }
      }
    }
    if (displayDAGCounters) {
      TezCounters counters = dagStatus.getDAGCounters();
      if (counters != null) {
        System.out.println("DAG Counters: " + counters);
      }
    }
  }

}
	
