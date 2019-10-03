package com.atguigu.hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CreateNameSpace {
  // 获取配置conf
  private Configuration configuration = HBaseConfiguration.create();
  // 微博内容表的表名
  private static final byte[] TABLE_CONTENT = Bytes.toBytes("weibo:content");
  // 用户关系表的表名
  private static final byte[] TABLE_RELATIONS = Bytes.toBytes("weibo:relations");
  // 微博收件箱的表名
  private static final byte[] TABLE_CONTENT_EMAIL = Bytes.toBytes("weibo:receive content email");

  public void initNameSpace() {
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(configuration);
      // 命名空间类似于关系型数据库中的schema，可以想象成文件夹
      NamespaceDescriptor weibo =
          NamespaceDescriptor.create("weibo")
              .addConfiguration("create", "Jinji")
              .addConfiguration("create time", System.currentTimeMillis() + "")
              .build();
      admin.createNamespace(weibo);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (admin != null) {
        try {
          admin.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void createTableContent() {
    HBaseAdmin admin = null;
  }
}
