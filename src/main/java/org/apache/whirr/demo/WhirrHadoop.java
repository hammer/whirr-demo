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

package org.apache.whirr.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.TokenCountMapper;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ServiceSpec;
import org.apache.whirr.service.ClusterSpec.InstanceTemplate;
import org.apache.whirr.service.hadoop.HadoopCluster;
import org.apache.whirr.service.hadoop.HadoopProxy;
import org.apache.whirr.service.hadoop.HadoopService;

public class WhirrHadoop {
  
  private String clusterName = "whirrdemo";
  
  private HadoopService service;
  private HadoopProxy proxy;
  private HadoopCluster cluster;
  
  public void startCluster() throws IOException {
    // Service
    String secretKeyFile;
    try {
       secretKeyFile = System.getProperty("whirr.ssh.keyfile");
    } catch (NullPointerException e) {
       secretKeyFile = System.getProperty("user.home") + "/.ssh/id_rsa";
    }
    ServiceSpec serviceSpec = new ServiceSpec();
    serviceSpec.setProvider(System.getProperty("whirr.provider", "ec2"));
    serviceSpec.setAccount(System.getProperty("whirr.user"));
    serviceSpec.setKey(System.getProperty("whirr.key"));
    serviceSpec.setSecretKeyFile(secretKeyFile);
    serviceSpec.setClusterName(clusterName);
    service = new HadoopService(serviceSpec);

    // Cluster
    ClusterSpec clusterSpec = new ClusterSpec(
	new InstanceTemplate(1, HadoopService.MASTER_ROLE),
	new InstanceTemplate(1, HadoopService.WORKER_ROLE));
    cluster = service.launchCluster(clusterSpec);

    // Proxy
    proxy = new HadoopProxy(serviceSpec, cluster);
    proxy.start();
  }
  
  public void runJob() throws Exception {
    Configuration conf = getConfiguration();
    
    JobConf job = new JobConf(conf, WhirrHadoop.class);
    JobClient client = new JobClient(job);
    waitForTaskTrackers(client);

    FileSystem fs = FileSystem.get(conf);
  
    OutputStream os = fs.create(new Path("input"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("b a\n");
    wr.close();
    
    job.setMapperClass(TokenCountMapper.class);
    job.setReducerClass(LongSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.setInputPaths(job, new Path("input"));
    FileOutputFormat.setOutputPath(job, new Path("output"));
    
    JobClient.runJob(job);
    FSDataInputStream in = fs.open(new Path("output/part-00000"));
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    reader.close();
  }
  
  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    for (Entry<Object, Object> entry : cluster.getConfiguration().entrySet()) {
      conf.set(entry.getKey().toString(), entry.getValue().toString());
    }
    return conf;
  }
  
  private static void waitForTaskTrackers(JobClient client) throws IOException {
    while (true) {
      ClusterStatus clusterStatus = client.getClusterStatus();
      int taskTrackerCount = clusterStatus.getTaskTrackers();
      if (taskTrackerCount > 0) {
	break;
      }
      try {
	System.out.print(".");
	Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }
  }
  
  public void stopCluster() throws IOException {
    proxy.stop();
    service.destroyCluster();
  }

  public static void main(String[] args) {
    WhirrHadoop wh = new WhirrHadoop();

    // Start the cluster
    System.out.println("Starting the cluster.");
    try {
      wh.startCluster();
    } catch (IOException e) {
      System.err.println("Could not start cluster: " + e.getMessage());
    }
    System.out.println("Cluster started.");

    // Run a job
    System.out.println("Running MapReduce job.");
    try {
      wh.runJob();
    } catch (Exception e) {
      System.err.println("Could not run job: " + e.getMessage());
    }
    System.out.println("Finished MapReduce job.");

    // Bring down the cluster
    System.out.println("Bringing down the cluster.");
    try {
      wh.stopCluster();
    } catch (IOException e) {
      System.err.println("Could not bring down the cluster: " + e.getMessage());
    }
    System.out.println("Cluster stopped.");
  }
}
