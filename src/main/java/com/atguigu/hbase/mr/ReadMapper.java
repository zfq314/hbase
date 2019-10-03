package com.atguigu.hbase.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/** 读取fruit的mapper */
public class ReadMapper extends TableMapper<ImmutableBytesWritable, Put> {
  @Override
  protected void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {

    // 获取value的相关数据cell
    Cell[] cells = value.rawCells();
    // 创建put用来存放数据
    Put put = new Put(key.get());
    // 遍历cells
    for (Cell cell : cells) {
      byte[] column = CellUtil.cloneQualifier(cell);
      if ("name".equals(Bytes.toString(column))) {
        put.add(cell);
      }
    }
    // 写出数据到reducer
    context.write(key, put);
  }
}
