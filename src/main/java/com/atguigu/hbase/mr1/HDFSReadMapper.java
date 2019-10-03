package com.atguigu.hbase.mr1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HDFSReadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String fields = value.toString();
    String[] columns = fields.split("\t");
    String rowkey = columns[0];
    String name = columns[1];
    String color = columns[2];

    Put put = new Put(Bytes.toBytes(rowkey));
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(color));
    context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put);
  }
}
