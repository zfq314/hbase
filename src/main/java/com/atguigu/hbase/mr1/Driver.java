package com.atguigu.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class Driver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
    Job job = Job.getInstance(configuration);
    job.setJarByClass(Driver.class);
    job.setMapperClass(HDFSReadMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    FileInputFormat.setInputPaths(job, new Path("/input_fruit"));
    job.setNumReduceTasks(100);
    TableMapReduceUtil.initTableReducerJob(
        "fruit_mr", HBaseWriteReducer.class, job, HRegionPartitioner.class);
    job.waitForCompletion(true);
  }
}
