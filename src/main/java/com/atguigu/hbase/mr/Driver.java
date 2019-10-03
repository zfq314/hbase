package com.atguigu.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class Driver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // job的创建
    // 创建configuration
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
    Job job = Job.getInstance(configuration);
    job.setJarByClass(Driver.class);

    // 设置mapper
    Scan scan = new Scan();
    TableMapReduceUtil.initTableMapperJob(
        "fruit", scan, ReadMapper.class, ImmutableBytesWritable.class, Put.class, job);
    // 设置reducer
    job.setNumReduceTasks(1000);
    TableMapReduceUtil.initTableReducerJob(
        "fruit_mr", WriteReducer.class, job, HRegionPartitioner.class);
    job.waitForCompletion(true);
  }
}
