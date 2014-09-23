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

import static org.apache.commons.lang.StringUtils.join;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;

/**
 * Simple TopK example which can take a CSV file and return the top K
 * elements in the given column.
 *
 * Use case: Given a CSV of user comments on a site listed as:
 * userid,postid,commentid,comment,timestamp
 * and we are looking for the top K commenter or the posts with the most comment
 */
public class TopK extends Configured implements Tool {

  private static final String INPUT = "input";
  private static final String WRITER = "writer";
  private static final String OUTPUT = "output";
  private static final String TOKENIZER = "tokenizer";
  private static final String SUM = "sum";

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TopK(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    TopK job = new TopK();
    if (args.length < 3) {
      printUsage();
      return 2;
    }
    if (job.run(args[0], args[1], args[2],
      args.length > 3 ? args[3] : "-1",
      args.length > 4 ? args[4] : "1", conf)) {
      return 0;
    }
    return 1;
  }

  private static void printUsage() {
    System.err.println(
      "Usage: topk <inputPath> <outputPath> <columnIndex, starting from 0> <K, -1 to all> <partition, default: 1>");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  private boolean run(String inputPath, String outputPath,
                      String columnIndex, String K, String partition, Configuration conf) throws Exception {
    TezConfiguration tezConf;
    if (conf != null) {
      tezConf = new TezConfiguration(conf);
    } else {
      tezConf = new TezConfiguration();
    }

    UserGroupInformation.setConfiguration(tezConf);

    // Create the TezClient to submit the DAG. Pass the tezConf that has all necessary global and
    // dag specific configurations
    TezClient tezClient = TezClient.create("topk", tezConf);
    // TezClient must be started before it can be used
    tezClient.start();

    try {
      DAG dag = createDAG(tezConf, inputPath, outputPath, columnIndex, K, partition);

      // check that the execution environment is ready
      tezClient.waitTillReady();
      // submit the dag and receive a dag client to monitor the progress
      DAGClient dagClient = tezClient.submitDAG(dag);

      // monitor the progress and wait for completion. This method blocks until the dag is done.
      DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
      // check success or failure and print diagnostics
      if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
        System.out.println("TopK failed with diagnostics: " + dagStatus.getDiagnostics());
        return false;
      }
      return true;
    } finally {
      // stop the client to perform cleanup
      tezClient.stop();
    }
  }

  private DAG createDAG(TezConfiguration tezConf, String inputPath, String outputPath,
                        String columnIndex, String top, String partition) throws IOException {

    DataSourceDescriptor dataSource = MRInput.createConfigBuilder(new Configuration(tezConf),
      TextInputFormat.class, inputPath).build();

    DataSinkDescriptor dataSink = MROutput.createConfigBuilder(new Configuration(tezConf),
      TextOutputFormat.class, outputPath).build();

    Vertex tokenizerVertex = Vertex.create(TOKENIZER,
      ProcessorDescriptor.create(TokenProcessor.class.getName())
        .setUserPayload(createPayload(Integer.valueOf(columnIndex))))
      .addDataSource(INPUT, dataSource);

    Vertex sumVertex = Vertex.create(SUM,
      ProcessorDescriptor.create(SumProcessor.class.getName()), Integer.valueOf(partition));

    // parallelism must be set to 1 as the writer needs to see the global picture of
    // the data set
    // multiple tasks from the writer will result in multiple list of the top K
    // elements as all task will take the partitioned data's top K element
    Vertex writerVertex = Vertex.create(WRITER,
      ProcessorDescriptor.create(Writer.class.getName())
        .setUserPayload(createPayload(Integer.valueOf(top))), 1)
      .addDataSink(OUTPUT, dataSink);

    OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
      .newBuilder(Text.class.getName(), IntWritable.class.getName(),
        HashPartitioner.class.getName()).build();

    // the edge uses a custom comparator to sort the data in descending order
    OrderedPartitionedKVEdgeConfig sorterEdgeConf = OrderedPartitionedKVEdgeConfig
      .newBuilder(IntWritable.class.getName(), Text.class.getName(),
        HashPartitioner.class.getName())
      .setKeyComparatorClass(KeyComparator.class.getName()).build();

    DAG dag = DAG.create("topk");
    return dag
      .addVertex(tokenizerVertex)
      .addVertex(sumVertex)
      .addVertex(writerVertex)
      .addEdge(Edge.create(tokenizerVertex, sumVertex, edgeConf.createDefaultEdgeProperty()))
      .addEdge(Edge.create(sumVertex, writerVertex, sorterEdgeConf.createDefaultEdgeProperty()));
  }

  private UserPayload createPayload(int num) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    dos.writeInt(num);
    dos.close();
    bos.close();
    ByteBuffer buffer = ByteBuffer.wrap(bos.toByteArray());
    return UserPayload.create(buffer);
  }

  /*
   * Example code to write a processor in Tez.
   * Processors typically apply the main application logic to the data.
   * TokenProcessor tokenizes the input data.
   * It uses an input that provide a Key-Value reader and writes
   * output to a Key-Value writer. The processor inherits from SimpleProcessor
   * since it does not need to handle any advanced constructs for Processors.
   */
  public static class TokenProcessor extends SimpleProcessor {

    private final IntWritable ONE = new IntWritable(1);
    private Text text = new Text();
    private int columnIndex;

    public TokenProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void initialize() throws Exception {
      // find out in which column we are looking for the top K elements
      byte[] payload = getContext().getUserPayload().deepCopyAsArray();
      ByteArrayInputStream bis = new ByteArrayInputStream(payload);
      DataInputStream dis = new DataInputStream(bis);
      columnIndex = dis.readInt();
      dis.close();
      bis.close();
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);
      // the recommended approach is to cast the reader/writer to a specific type instead
      // of casting the input/output. This allows the actual input/output type to be replaced
      // without affecting the semantic guarantees of the data type that are represented by
      // the reader and writer.
      // The inputs/outputs are referenced via the names assigned in the DAG.
      KeyValueReader kvReader = (KeyValueReader) getInputs().get(INPUT).getReader();
      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(SUM).getWriter();
      while (kvReader.next()) {
        String[] split = kvReader.getCurrentValue().toString().split(",");
        if (split.length > columnIndex) {
          text.set(split[columnIndex]);
          kvWriter.write(text, ONE);
        }
      }
    }
  }

  /**
   * Example code to sum the words, which needed to be sorted later in descending order.
   */
  public static class SumProcessor extends SimpleProcessor {

    public SumProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);
      // The KeyValues reader provides all values for a given key. The aggregation of values per key
      // is done by the LogicalInput. Since the key is the word and the values are its counts in
      // the different TokenProcessors, summing all values per key provides the sum for that word.
      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(WRITER).getWriter();
      KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(TOKENIZER).getReader();
      while (kvReader.next()) {
        Text word = (Text) kvReader.getCurrentKey();
        int sum = 0;
        for (Object value : kvReader.getCurrentValues()) {
          sum += ((IntWritable) value).get();
        }
        kvWriter.write(new IntWritable(sum), word);
      }
    }
  }

  /**
   * Takes the first K element coming from the {@link SumProcessor}
   * if K is specified, otherwise it writes all the data in a sorted order.
   * If there are multiple values with the same count it will join them with a comma.
   */
  public static class Writer extends SimpleMRProcessor {

    private int top;
    private int current;

    public Writer(ProcessorContext context) {
      super(context);
    }

    @Override
    public void initialize() throws Exception {
      byte[] payload = getContext().getUserPayload().deepCopyAsArray();
      ByteArrayInputStream bis = new ByteArrayInputStream(payload);
      DataInputStream dis = new DataInputStream(bis);
      top = dis.readInt();
      // if the K is -1 it will write Integer.MAX_VALUE of data to the data sink
      top = top == -1 ? Integer.MAX_VALUE : top;
      dis.close();
      bis.close();
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);
      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT).getWriter();
      KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(SUM).getReader();
      if (top > 0) {
        while (kvReader.next()) {
          if (current < top) {
            current++;
            List<String> words = new ArrayList<String>();
            for (Object word : kvReader.getCurrentValues()) {
              words.add(word.toString());
            }
            // if multiple values has the same count join them
            kvWriter.write(join(words, ','), kvReader.getCurrentKey());
          } else {
            break;
          }
        }
      }
    }
  }

  /**
   * Custom comparator which order the words by descending order. A simple
   * trick used by switching the parameters.
   */
  public static class KeyComparator implements RawComparator<IntWritable> {

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return WritableComparator.compareBytes(b2, s2, l2, b1, s1, l1);
    }

    @Override
    public int compare(IntWritable intWritable, IntWritable intWritable2) {
      return intWritable2.compareTo(intWritable);
    }
  }
}
