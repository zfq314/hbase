import com.atguigu.hbase.uitls.HBaseClientUtil;
import org.junit.Test;

import java.io.IOException;

public class HBaseClientUtilTest {
  @Test
  public void isExists() throws IOException {
    boolean exists = HBaseClientUtil.isExistsTab("staff1");
    System.out.println(exists);
  }

  @Test
  public void crateTable() throws IOException {
    boolean result = HBaseClientUtil.createTable("jqtest", "info", "junit");
    System.out.println(result);
  }

  @Test
  public void modifyVersion() throws IOException {
    String result = HBaseClientUtil.modifyVersion("jqtest", "junit", 5);
    System.out.println(result);
  }

  @Test
  public void deleteTable() throws IOException {
    String result = HBaseClientUtil.deleteTable("weibo:content");
    System.out.println(result);
  }

  @Test
  public void getRow() throws IOException {
    String result = HBaseClientUtil.getRow("fruit", "1003");
    System.out.println(result);
  }

  @Test
  public void getScannInfo() throws IOException {
    String result = HBaseClientUtil.getScanInfo("fruit");
    System.out.println(result);
  }

  @Test
  public void putData() throws IOException {
    String result = HBaseClientUtil.putData("fruit", "1005", "info", "size", "50px");
    System.out.println(result);
  }

  @Test
  public void getScanFilter() throws IOException {
    HBaseClientUtil.getScannFilter("fruit", "info", "name", "Pear");
  }

  @Test
  public void deleteRow() throws IOException {
    boolean result = HBaseClientUtil.deleteRow("fruit", "10060");
    System.out.println(result);
  }

  @Test
  public void deleteByColumn() throws IOException {
    boolean result = HBaseClientUtil.deleteByColumn("fruit", "1005", "info", "size");
    System.out.println(result);
  }
}
