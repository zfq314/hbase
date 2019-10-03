import com.atguigu.hbase.api.HBaseUtils;
import org.junit.Test;

import java.io.IOException;

public class HBaseUtilsTest {
  @Test
  public void setExistTest() throws IOException {
    boolean exist = HBaseUtils.isTableExist("student");
    System.out.println(exist);
  }

  @Test
  public void createTab() throws IOException {
    HBaseUtils.createTable("student", "info");
    HBaseUtils.createTable("test", "info");
  }

  @Test
  public void modidyColumnVersin() throws IOException {
    HBaseUtils.modifyFamilyVersion("test", "info", 10);
    HBaseUtils.modifyFamilyVersion("last", "info", 5);
  }

  @Test
  public void deleteTab() throws IOException {
    HBaseUtils.deleteTab("test");
  }

  @Test
  public void getRows() throws IOException {
    HBaseUtils.getRow("student", "1001");
  }

  @Test
  public void getScanner() throws IOException {
    HBaseUtils.getScan("student");
  }

  @Test
  public void put() throws IOException {
    HBaseUtils.putData("student", "1004", "info", "address", "河南");
    HBaseUtils.putData("student", "1005", "info", "address", "重庆");
  }

  @Test
  public void filter() throws IOException {
    HBaseUtils.getScanFilter("student", "info", "name", "xxh");
  }

  @Test
  public void delete() throws IOException {
    HBaseUtils.deleteColumn("student", "1006");
  }

  @Test
  public void deleteColumn() throws IOException {
    HBaseUtils.deleteByColumn("student", "1001", "info", "name");
  }
}
