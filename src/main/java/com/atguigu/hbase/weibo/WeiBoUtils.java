package com.atguigu.hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WeiBoUtils {
  private static Connection connection = null;

  static {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

    configuration.set("hbase.zookeeper.property.clientPort", "2181");
    try {
      connection = ConnectionFactory.createConnection(configuration);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  // 创建命名空间
  public static void createNamespace(String namespace) throws IOException {
    // 创建连接

    Admin admin = connection.getAdmin();
    // 获取namespace的描述器
    NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
    // 创建操作
    admin.createNamespace(namespaceDescriptor);
    // 关闭资源
    admin.close();
  }

  // 创建表
  public static void createTable(String tableName, int versions, String... cloumnFamilies)
      throws IOException {
    Admin admin = connection.getAdmin();

    // 获取表描述器
    HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
    // 循环添加列族
    for (String cloumnFamily : cloumnFamilies) {
      // 获取列描述器
      HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cloumnFamily);
      hColumnDescriptor.setMaxVersions(versions);
      hTableDescriptor.addFamily(hColumnDescriptor);
    }
    admin.createTable(hTableDescriptor);
    admin.close();
  }

  /**
   * 1. 更新微博内容表 2.更新收件箱表数据 获取当前操作人的fans 去往收件箱更新内容数据
   *
   * @param uid
   * @param content
   * @throws IOException
   */
  // 发布微博
  public static void createData(String uid, String content) throws IOException {
    // 获取三张操作的表对象
    Table contentTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
    Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
    Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));

    // 拼接RK
    long ts = System.currentTimeMillis();
    String rowKey = uid + "_" + ts;
    // 生成put对象
    Put put = new Put(Bytes.toBytes(rowKey));
    // 内容表添加数据
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));
    contentTable.put(put);
    // 关系表中的fances
    Get get = new Get(Bytes.toBytes(uid));
    get.addFamily(Bytes.toBytes("fans"));
    Result result = relaTable.get(get);
    Cell[] cells = result.rawCells();
    if (cells.length <= 0) {
      return;
    }
    // 更新fans收件箱
    List<Put> puts = new ArrayList<>();
    for (Cell cell : cells) {
      byte[] qualifier = CellUtil.cloneQualifier(cell);
      Put inboxPut = new Put(qualifier);
      inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
      puts.add(inboxPut);
    }
    inboxTable.put(puts);
    // 关闭资源
    inboxTable.close();
    relaTable.close();
    contentTable.close();
  }

  /**
   * 1.在用户关系表，添加操作人的attends 被操作人的fans 2.在收件箱表中 在微博内容表中被关注者3条数据（rowkey） 收件箱表中添加操作人的关注者信息
   *
   * @param uid
   * @param uids
   */
  // 关注用户
  public static void addAttend(String uid, String... uids) throws IOException {
    // 获取三张操作的表对象
    Table contentTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
    Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
    Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
    // 创建操者的put对象
    Put relaPut = new Put(Bytes.toBytes(uid));
    ArrayList<Put> puts = new ArrayList<>();
    for (String s : uids) {
      relaPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(s), Bytes.toBytes(s));
      // 创建被关注者的对象的put
      Put fansPut = new Put(Bytes.toBytes(s));
      fansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
      puts.add(fansPut);
    }
    puts.add(relaPut);
    relaTable.put(puts);

    Put inboxPut = new Put(Bytes.toBytes(uid));
    // 获取内容表中被关注者的rowKey
    for (String s : uids) {
      Scan scan = new Scan(Bytes.toBytes(s), Bytes.toBytes(s + "|"));
      ResultScanner scanner = contentTable.getScanner(scan);
      for (Result result : scanner) {
        String rowKey = Bytes.toString(result.getRow());
        String[] split = rowKey.split("_");
        byte[] row = result.getRow();
        inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(s), Long.parseLong(split[1]), row);
      }
    }
    inboxTable.put(inboxPut);
    inboxTable.close();
    relaTable.close();
    contentTable.close();
  }
  /** 1.用户关系表 删除操作者关注列族的待取关用户 删除待取关用户fans的操作者 2.收件箱表 删除操作这的待取关用户的信息 */

  // 取关用户
  public static void delAttend(String uid, String... uids) throws IOException {
    Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
    Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
    // 操作者的删除的对象
    Delete relaDel = new Delete(Bytes.toBytes(uid));
    ArrayList<Delete> deletes = new ArrayList<>();
    for (String s : uids) {
      // 被取关者的对象
      Delete fansDel = new Delete(Bytes.toBytes(s));
      fansDel.addColumns(Bytes.toBytes("fans"), Bytes.toBytes(uid));
      relaDel.addColumns(Bytes.toBytes("attends"), Bytes.toBytes(s));
      deletes.add(fansDel);
    }
    deletes.add(relaDel);
    // 删除操作

    relaTable.delete(deletes);

    Delete inboxDel = new Delete(Bytes.toBytes(uid));
    for (String s : uids) {
      inboxDel.addColumns(Bytes.toBytes("info"), Bytes.toBytes(s));
    }
    // 执行收件箱表删除操作

    inboxTable.delete(inboxDel);

    // 关闭资源
    inboxTable.close();
    relaTable.close();
  }

  // 获取微博内容（初始化内容）
  public static void getInit(String uid) throws IOException {
    // 获取表对象（2）
    Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
    Table contentTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
    // 获取收件箱表数据
    Get get = new Get(Bytes.toBytes(uid)); // get收件箱对象
    get.setMaxVersions(); // 设置获最大版本的数据

    Result result = inboxTable.get(get);

    ArrayList<Get> gets = new ArrayList<>();
    Cell[] cells = result.rawCells();
    // 遍历返回内容并将其封装成get对象
    for (Cell cell : cells) {
      Get contentGet = new Get(CellUtil.cloneValue(cell));
      gets.add(contentGet);
    }
    // 根据收件箱取值去往内容表获取微博内容
    Result[] results = contentTable.get(gets);
    for (Result result1 : results) {
      Cell[] cells1 = result1.rawCells();
      for (Cell cell : cells1) {
        System.out.println(
            "RK:"
                + Bytes.toString(CellUtil.cloneRow(cell))
                + ",Content:"
                + Bytes.toString(CellUtil.cloneValue(cell)));
      }
    }
    // 关闭资源
    inboxTable.close();
    contentTable.close();
  }

  // 获取微博内容（查看某个人所有的微博内容）
  public static void getData(String uid) throws IOException {
    // 获取表对象
    Table table = connection.getTable(TableName.valueOf(Constant.CONTENT));
    // 扫描
    Scan scan = new Scan();
    // 用过滤器过滤
    RowFilter rowFilter =
        new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));
    scan.setFilter(rowFilter);
    ResultScanner scanner = table.getScanner(scan);
    // 遍历打印
    for (Result result : scanner) {
      Cell[] cells = result.rawCells();
      for (Cell cell : cells) {
        System.out.println(
            "RK:"
                + Bytes.toString(CellUtil.cloneRow(cell))
                + ",Content:"
                + Bytes.toString(CellUtil.cloneValue(cell)));
      }
    }
    // 关闭资源
    table.close();
  }
}
