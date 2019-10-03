package com.atguigu.hbase.weibo;

import java.io.IOException;

public class WeiBo {
  public static void init() throws IOException {
    // 创建相关命名空间&表
    WeiBoUtils.createNamespace(Constant.NAMESPACE);
    // 创建内容表
    WeiBoUtils.createTable(Constant.CONTENT, 1, "info");
    // 创建用户关系表
    WeiBoUtils.createTable(Constant.RELATIONS, 1, "attends", "fans");
    // 创建收件箱表，多版本
    WeiBoUtils.createTable(Constant.INBOX, 2, "info");
  }

  public static void main(String[] args) throws IOException {
    // 测试
    // init();
    // 1001,1002发微博
    // WeiBoUtils.createData("1001", "今天天气挺好的");
    // WeiBoUtils.createData("1002", "今天天气不好");
    // 1001关注1002,1003
    // WeiBoUtils.addAttend("1001", "1002", "1003");
    // 获取1001初始化信息
    // WeiBoUtils.getInit("1001");
    // 1003发布微博
    // WeiBoUtils.createData("1003", "小红 ");
    // WeiBoUtils.createData("1003", "小紫 ");
    // WeiBoUtils.createData("1003", "红红 ");
    // 获取1001初始化信息
    WeiBoUtils.getInit("1001");
    // 取消关注
    WeiBoUtils.delAttend("1001", "1002");
    WeiBoUtils.getInit("1001");
  }
}
