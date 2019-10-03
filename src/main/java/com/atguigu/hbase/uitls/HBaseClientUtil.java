package com.atguigu.hbase.uitls;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseClientUtil {
  // 需要先连接资源，连接的时候比较消资源，链接代码设置成代码块
  private static Connection connection;

  static {
    // 连接的配置，先设置连接的配置资源
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
    configuration.set("hbase.zookeeper.property.clientPort", "2181");
    try {
      // 根据配置获取链接
      connection = ConnectionFactory.createConnection(configuration);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 校验表是否存在
   *
   * @param tableName
   * @return
   * @throws IOException
   */
  public static boolean isExistsTab(String tableName) throws IOException {
    // 获取管理员的权限
    Admin admin = connection.getAdmin();
    return admin.tableExists(TableName.valueOf(tableName));
  }

  /**
   * 创建表
   *
   * @param tableName 表名
   * @param columnFamilies 可变参数 不固定的列族
   * @return
   * @throws IOException
   */
  public static boolean createTable(String tableName, String... columnFamilies) throws IOException {
    // 管理员操作
    Admin admin = connection.getAdmin();
    if (isExistsTab(tableName)) {
      System.out.println(tableName + "表已经存在，不能重新建表");
      return false;
    } else {
      // 获取表描述器
      HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
      // 遍历列族
      for (String columnFamily : columnFamilies) {
        // 获取一个列描述器
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
        hTableDescriptor.addFamily(columnDescriptor);
      }

      admin.createTable(hTableDescriptor);
      System.out.println(tableName + "表创建成功");
      admin.close();
      return true;
    }
  }

  /**
   * 修改版本
   *
   * @param tableName
   * @param columnFamily
   * @param version
   * @return
   * @throws IOException
   */
  public static String modifyVersion(String tableName, String columnFamily, int version)
      throws IOException {
    if (isExistsTab(tableName)) {
      Admin admin = connection.getAdmin();
      HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
      // 设置修改的版本
      hColumnDescriptor.setMaxVersions(version);
      admin.modifyColumn(TableName.valueOf(tableName), hColumnDescriptor);
      admin.close();
      return tableName + "表的" + columnFamily + "列族版本修改成功";
    } else {
      return tableName + "表不存在，不能修改版本吧";
    }
  }

  /**
   * 删除表，先关表
   *
   * @param tableName
   * @return
   * @throws IOException
   */
  public static String deleteTable(String tableName) throws IOException {
    if (isExistsTab(tableName)) {
      Admin admin = connection.getAdmin();
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
      admin.close();
      return tableName + "表删除成功";
    } else {
      return tableName + "表不存在";
    }
  }

  /**
   * 获取一列数据
   *
   * @param tableName
   * @param rowKey
   * @return
   * @throws IOException
   */
  public static String getRow(String tableName, String rowKey) throws IOException {
    if (isExistsTab(tableName)) {
      Table table = connection.getTable(TableName.valueOf(tableName));
      Get get = new Get(Bytes.toBytes(rowKey));
      Result result = table.get(get);
      Cell[] cells = result.rawCells();
      String info = null;
      info = getString(info, cells);
      table.close();
      return info;
    } else {
      return tableName + "表不存在";
    }
  }

  /**
   * 扫描整个表
   *
   * @param tableName
   * @throws IOException
   */
  public static String getScanInfo(String tableName) throws IOException {
    if (isExistsTab(tableName)) {
      // 获取表
      Table table = connection.getTable(TableName.valueOf(tableName));
      Scan scan = new Scan();
      ResultScanner scanner = table.getScanner(scan);
      String info = null;
      for (Result result : scanner) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
          byte[] row = CellUtil.cloneRow(cell);
          byte[] family = CellUtil.cloneFamily(cell);
          byte[] qualifier = CellUtil.cloneQualifier(cell);
          byte[] value = CellUtil.cloneValue(cell);

          info =
              "rowKey:"
                  + Bytes.toString(row)
                  + "，列族为family:"
                  + Bytes.toString(family)
                  + ",列名colum:"
                  + Bytes.toString(qualifier)
                  + ",值value:"
                  + Bytes.toString(value);
        }
      }
      table.close();
      return info;
    } else {
      return tableName + "表不存在";
    }
  }

  /**
   * 添加数据
   *
   * @param tableName
   * @param rowKey
   * @param family
   * @param column
   * @param value
   * @return
   * @throws IOException
   */
  public static String putData(
      String tableName, String rowKey, String family, String column, String value)
      throws IOException {
    if (isExistsTab(tableName)) {

      Table table = connection.getTable(TableName.valueOf(tableName));
      Put put = new Put(Bytes.toBytes(rowKey));
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
      table.put(put);

      table.close();
      return "添加成功,添加的值信息：" + value;
    } else {
      return tableName + "表不存在";
    }
  }

  /**
   * 根据过滤，获取所有数据
   *
   * @param tableName
   * @param family
   * @param column
   * @param value
   * @return
   * @throws IOException
   */
  public static void getScannFilter(String tableName, String family, String column, String value)
      throws IOException {
    if (isExistsTab(tableName)) {
      Table table = connection.getTable(TableName.valueOf(tableName));
      Scan scan = new Scan();
      // 查询的时候添加过滤
      SingleColumnValueFilter singleColumnValueFilter =
          new SingleColumnValueFilter(
              Bytes.toBytes(family),
              Bytes.toBytes(column),
              CompareFilter.CompareOp.EQUAL,
              Bytes.toBytes(value));
      // 没有字段的符号过滤
      singleColumnValueFilter.setFilterIfMissing(true);
      scan.setFilter(singleColumnValueFilter);
      ResultScanner scanner = table.getScanner(scan);

      for (Result result : scanner) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
          byte[] row = CellUtil.cloneRow(cell);
          byte[] familes = CellUtil.cloneFamily(cell);
          byte[] qualifier = CellUtil.cloneQualifier(cell);
          byte[] values = CellUtil.cloneValue(cell);
          System.out.println(
              Bytes.toString(row)
                  + ":"
                  + Bytes.toString(familes)
                  + ","
                  + Bytes.toString(qualifier)
                  + ","
                  + Bytes.toString(values));
        }
      }
      table.close();
    } else {
      System.out.println(tableName + "表不存在");
    }
  }

  /**
   * 删除一行数据
   *
   * @param tableName
   * @param rowKey
   * @return
   * @throws IOException
   */
  public static boolean deleteRow(String tableName, String rowKey) throws IOException {
    if (isExistsTab(tableName)) {
      Table table = connection.getTable(TableName.valueOf(tableName));
      Delete delete = new Delete(Bytes.toBytes(rowKey));
      table.delete(delete);
      System.out.println("删除单行数据");
      table.close();
      return true;
    } else {
      System.out.println(tableName + "表不存在");
      return false;
    }
  }

  /**
   * 删除单列的数据
   *
   * @param tableName
   * @param rowKey
   * @param family
   * @param column
   * @return
   * @throws IOException
   */
  public static boolean deleteByColumn(
      String tableName, String rowKey, String family, String column) throws IOException {
    if (isExistsTab(tableName)) {
      Table table = connection.getTable(TableName.valueOf(tableName));
      Delete delete = new Delete(Bytes.toBytes(rowKey));
      delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
      table.delete(delete);
      table.close();
      return true;
    } else {
      System.out.println(tableName + "表不存在");
      return false;
    }
  }

  /**
   * 遍历数据的方法
   *
   * @param info
   * @param cells
   * @return
   */
  private static String getString(String info, Cell[] cells) {
    for (Cell cell : cells) {
      byte[] row = CellUtil.cloneRow(cell);
      byte[] familes = CellUtil.cloneFamily(cell);
      byte[] qualifier = CellUtil.cloneQualifier(cell);
      byte[] values = CellUtil.cloneValue(cell);
      info =
          Bytes.toString(row)
              + ":"
              + Bytes.toString(familes)
              + ","
              + Bytes.toString(qualifier)
              + ","
              + Bytes.toString(values);
    }
    return info;
  }
}
