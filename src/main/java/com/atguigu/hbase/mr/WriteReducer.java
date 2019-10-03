package com.atguigu.hbase.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class WriteReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
  // 遍历之后，然后直接写出

  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context)
      throws IOException, InterruptedException {
    for (Put value : values) {
      context.write(NullWritable.get(), value);
    }
  }
}
