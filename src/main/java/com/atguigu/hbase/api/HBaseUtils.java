package com.atguigu.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtils {
  private static Connection connection = null;

  static {
    try {
      // 单例方法实例化
      Configuration configuration = HBaseConfiguration.create();
      configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

      configuration.set("hbase.zookeeper.property.clientPort", "2181");
      connection = ConnectionFactory.createConnection(configuration);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 校验表是否存在
   *
   * @param tabName
   * @return
   * @throws IOException
   */
  public static boolean isTableExist(String tabName) throws IOException {
    Admin admin = connection.getAdmin();
    admin.close();
    return admin.tableExists(TableName.valueOf(tabName));
  }

  /**
   * 创建表
   *
   * @param tabName
   * @param columnFamilies
   * @throws IOException
   */
  public static void createTable(String tabName, String... columnFamilies) throws IOException {
    // 验证表是否存在
    boolean exist = isTableExist(tabName);
    if (exist) {
      System.out.println("表已经存在");
    } else {

      // 获取admin
      Admin admin = connection.getAdmin();
      // 创建表
      // 获取表描述器
      HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tabName));
      for (String family : columnFamilies) {
        // 获取列描述器
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
        hTableDescriptor.addFamily(hColumnDescriptor);
      }
      admin.createTable(hTableDescriptor);
      admin.close();
    }
  }

  /**
   * 修改版本
   *
   * @param tabName
   * @param columnFamily
   * @param versions
   */
  public static void modifyFamilyVersion(String tabName, String columnFamily, int versions)
      throws IOException {
    // 校验表是否存在
    boolean exist = isTableExist(tabName);
    if (exist) {
      // 获取admin
      Admin admin = connection.getAdmin();
      HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
      hColumnDescriptor.setMaxVersions(versions);
      // 修改版本
      admin.modifyColumn(TableName.valueOf(tabName), hColumnDescriptor);
      admin.close();
    } else {
      System.out.println(tabName + "表不存在");
    }
  }

  /**
   * 删除一个表
   *
   * @param tabName
   * @throws IOException
   */
  public static void deleteTab(String tabName) throws IOException {
    boolean exist = isTableExist(tabName);
    if (exist) {
      Admin admin = connection.getAdmin();

      // 关表，删表
      admin.disableTable(TableName.valueOf(tabName));
      admin.deleteTable(TableName.valueOf(tabName));

      admin.close();
      System.out.println("删除成功");
    } else {
      System.out.println(tabName + "表不存在");
    }
  }

  /**
   * 查询一列数据对象
   *
   * @param tabName
   * @param rowKey
   * @throws IOException
   */
  public static void getRow(String tabName, String rowKey) throws IOException {
    boolean exist = isTableExist(tabName);
    if (exist) {
      // 获取表的连接对象
      Table table = connection.getTable(TableName.valueOf(tabName));

      // 获取表的连接对象，然后查询表
      Get get = new Get(Bytes.toBytes(rowKey));
      Result result = table.get(get);
      // 获取基础的cell信息
      Cell[] cells = result.rawCells();
      // 遍历cell
      for (Cell cell : cells) {
        // 获取相关的信息
        byte[] row = CellUtil.cloneRow(cell);
        byte[] family = CellUtil.cloneFamily(cell);
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        byte[] value = CellUtil.cloneValue(cell);
        System.out.println(
            rowKey
                + ","
                + Bytes.toString(family)
                + ","
                + Bytes.toString(qualifier)
                + ","
                + Bytes.toString(value));
      }
      table.close();
    } else {
      System.out.println(tabName + "表不存在");
    }
  }

  /**
   * 扫描表
   *
   * @param tabName
   * @throws IOException
   */
  public static void getScan(String tabName) throws IOException {
    boolean exist = isTableExist(tabName);
    if (exist) {
      // 获取表

      Table table = connection.getTable(TableName.valueOf(tabName));
      Scan scan = new Scan();
      ResultScanner scanner = table.getScanner(scan);
      for (Result result : scanner) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
          // 获取相关的信息
          byte[] row = CellUtil.cloneRow(cell);
          byte[] family = CellUtil.cloneFamily(cell);
          byte[] qualifier = CellUtil.cloneQualifier(cell);
          byte[] value = CellUtil.cloneValue(cell);
          System.out.println(
              Bytes.toString(row)
                  + "::"
                  + Bytes.toString(family)
                  + ","
                  + Bytes.toString(qualifier)
                  + ","
                  + Bytes.toString(value));
        }
      }
      // 关闭表
      table.close();
    } else {
      System.out.println(tabName + "表不存在");
    }
  }

  /**
   * 添加数据
   *
   * @param tabName
   * @param rowKey
   * @param columnFamily
   * @param column
   * @param value
   * @throws IOException
   */
  public static void putData(
      String tabName, String rowKey, String columnFamily, String column, String value)
      throws IOException {
    boolean exist = isTableExist(tabName);
    if (exist) {
      Table table = connection.getTable(TableName.valueOf(tabName));
      Put put = new Put(Bytes.toBytes(rowKey));
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
      table.put(put);
      table.close();
    } else {
      System.out.println(tabName + "表不存在");
    }
  }
  // 扫描数据的时候，添加过滤器
  public static void getScanFilter(String tabName, String columnFamily, String column, String value)
      throws IOException {
    if (isTableExist(tabName)) {
      // 获取连接
      Table table = connection.getTable(TableName.valueOf(tabName));
      Scan scan = new Scan();
      SingleColumnValueFilter singleColumnValueFilter =
          new SingleColumnValueFilter(
              Bytes.toBytes(columnFamily),
              Bytes.toBytes(column),
              CompareFilter.CompareOp.EQUAL,
              Bytes.toBytes(value));
      // 将没有字段的符号过滤掉
      singleColumnValueFilter.setFilterIfMissing(true);
      scan.setFilter(singleColumnValueFilter);

      ResultScanner scanner = table.getScanner(scan);
      for (Result result : scanner) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
          // 获取相关的信息
          byte[] row = CellUtil.cloneRow(cell);
          byte[] family = CellUtil.cloneFamily(cell);
          byte[] qualifier = CellUtil.cloneQualifier(cell);
          byte[] values = CellUtil.cloneValue(cell);
          System.out.println(
              Bytes.toString(row)
                  + ","
                  + Bytes.toString(family)
                  + ","
                  + Bytes.toString(qualifier)
                  + ","
                  + Bytes.toString(values));
        }
      }
      table.close();

    } else {
      System.out.println(tabName + "表不存在");
    }
  }

  /**
   * 删除一行数据
   *
   * @param tabName
   * @param rowKey
   */
  public static void deleteColumn(String tabName, String rowKey) throws IOException {
    if (isTableExist(tabName)) {
      Table table = connection.getTable(TableName.valueOf(tabName));
      Delete delete = new Delete(Bytes.toBytes(rowKey));
      table.delete(delete);
      table.close();
    } else {
      System.out.println(tabName + "表不存在");
    }
  }

  /**
   * 删除某一行的一列数据
   *
   * @param tabName
   * @param rowKey
   * @param columnFamily
   * @param column
   */
  public static void deleteByColumn(
      String tabName, String rowKey, String columnFamily, String column) throws IOException {
    if (isTableExist(tabName)) {
      Table table = connection.getTable(TableName.valueOf(tabName));
      Delete delete = new Delete(Bytes.toBytes(rowKey));
      delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
      table.delete(delete);
      table.close();
    } else {
      System.out.println(tabName + "表不存在");
    }
  }
}
