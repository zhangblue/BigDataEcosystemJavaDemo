package zhangblue.hbase.demo;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import zhangblue.hbase.repository.HBaseResources;

public class ExampleDemo {

  private HBaseResources hBaseResources;

  public ExampleDemo(HBaseResources hBaseResources) {
    this.hBaseResources = hBaseResources;
  }

  /***
   * 得到制定列族下的所有信息
   * @param tableName 表名
   * @param family 列族名
   * @param rowKey rowkey
   */
  public void getByRowKey(String tableName, String family, String rowKey) {
    try {
      Table table = hBaseResources.getConnection().getTable(TableName.valueOf(tableName));
      Get get = new Get(Bytes.toBytes(rowKey));
      get.addFamily(Bytes.toBytes(family));
      Result result = table.get(get);

      for (Cell cell : result.listCells()) {

        System.out.println("family = " +
            Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));

        System.out.println("qualifier = " + Bytes
            .toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                cell.getQualifierLength()));

        System.out.println("value = " + Bytes
            .toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /***
   * 得到制定列下的所有信息
   * @param tableName 表名
   * @param family 列族名
   * @param rowKey rowkey
   */
  public void getByRowKey(String tableName, String family, String qualifier, String rowKey) {
    try {
      Table table = hBaseResources.getConnection().getTable(TableName.valueOf(tableName));
      Get get = new Get(Bytes.toBytes(rowKey));
      get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
      Result result = table.get(get);
      if (result.rawCells().length > 0) {
        Cell cell = result.rawCells()[0];
        System.out.println("family = " +
            Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
        System.out.println("qualifier = " + Bytes
            .toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                cell.getQualifierLength()));
        System.out.println("value = " + Bytes
            .toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
      } else {
        System.out.println("rowkey not exists");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  /***
   * 创建有数据生命周期的hbase表
   * @param tableName 表名
   * @param family 列族名
   * @param time 存活时间(秒)
   * @throws Exception
   */
  public boolean createHbaseTable(String tableName, String family, int time) {
    boolean flage = false;
    try {
      Admin admin = hBaseResources.getConnection().getAdmin();
      if (admin.tableExists(TableName.valueOf(tableName))) {
        flage = false;
      } else {
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
        if (time > 0) {
          hColumnDescriptor.setTimeToLive(time);
        }
        desc.addFamily(hColumnDescriptor);
        admin.createTable(desc);
        flage = true;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return flage;
  }

  /**
   * 数据插入hbase
   *
   * @param tableName hbase表名
   * @param family 列族
   * @param qualifier 列
   * @param rowKye rowkey
   * @param value value
   */
  public void putToHbase(String tableName, String family, String qualifier, String rowKye,
      String value) {
    try {
      Table table = hBaseResources.getConnection().getTable(TableName.valueOf(tableName));
      Put put = new Put(Bytes.toBytes(rowKye));
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
      table.put(put);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /***
   * 删除hbase表
   * @param tableName hbase表名
   */
  public void deleteTable(String tableName) {
    try {
      Admin admin = hBaseResources.getConnection().getAdmin();
      admin.disableTable(TableName.valueOf(tableName));
      if (admin.isTableDisabled(TableName.valueOf(tableName))) {
        admin.deleteTable(TableName.valueOf(tableName));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
