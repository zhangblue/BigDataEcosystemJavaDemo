package zhangblue.hbase.demo;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
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
  public void getByRowKey(String tableName, String family, String rowKey) throws IOException {
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
  }

  /***
   * 得到制定列下的所有信息
   * @param tableName 表名
   * @param family 列族名
   * @param rowKey rowkey
   */
  public void getByRowKey(String tableName, String family, String qualifier, String rowKey) throws IOException {
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
  }


  /***
   * 创建有数据生命周期的hbase表
   * @param tableName 表名
   * @param family 列族名
   * @param time 存活时间(秒)
   * @throws Exception
   */
  public boolean createHbaseTable(String tableName, String family, int time) throws IOException {
    boolean flage = false;
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
      String value) throws IOException {
    Table table = hBaseResources.getConnection().getTable(TableName.valueOf(tableName));
    Put put = new Put(Bytes.toBytes(rowKye));
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
    table.put(put);
  }

  /***
   * 删除hbase表
   * @param tableName hbase表名
   */
  public void deleteTable(String tableName) throws IOException {
    Admin admin = hBaseResources.getConnection().getAdmin();
    admin.disableTable(TableName.valueOf(tableName));
    if (admin.isTableDisabled(TableName.valueOf(tableName))) {
      admin.deleteTable(TableName.valueOf(tableName));
    }
  }

  /**
   * 遍历hbase表，并输出每个表的family
   */
  public void listTables() throws IOException {
    Admin admin = hBaseResources.getConnection().getAdmin();
    HTableDescriptor[] hTableDescriptors = admin.listTables();

    for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
      Collection<HColumnDescriptor> columnDescriptors = hTableDescriptor.getFamilies();
      Iterator<HColumnDescriptor> iterable = columnDescriptors.iterator();
      System.out.println("table name = " + hTableDescriptor.getTableName().toString());
      while (iterable.hasNext()) {
        HColumnDescriptor hColumnDescriptor = iterable.next();
        System.out.println("family name = " + hColumnDescriptor.getNameAsString());
      }
      System.out.println("--------分隔符----------");
    }
  }

  /**
   * 给table添加列族
   *
   * @param tableName 表名
   * @param family 列族名
   */
  public void tableAddFamily(String tableName, String family) throws IOException {
    Admin admin = hBaseResources.getConnection().getAdmin();
    if (admin.isTableEnabled(TableName.valueOf(tableName))) {
      System.out.println("disable table = " + tableName);
      admin.disableTable(TableName.valueOf(tableName));
    }
    HColumnDescriptor cf = new HColumnDescriptor(Bytes.toBytes(family));
    admin.addColumn(TableName.valueOf(tableName), cf);
    if (admin.isTableDisabled(TableName.valueOf(tableName))) {
      System.out.println("enable table = " + tableName);
      admin.enableTable(TableName.valueOf(tableName));
    }

  }

  /**
   * 按照rowkey删除数据
   *
   * @param tableName 表名
   * @param rowKey rowkey
   */
  public void deleteByRowKey(String tableName, String rowKey) throws IOException {
    Table table = hBaseResources.getConnection().getTable(TableName.valueOf(tableName));
    Delete delete = new Delete(Bytes.toBytes(rowKey));
    table.delete(delete);
  }

  /**
   * 清空列族下的所有列
   *
   * @param tableName 表名
   * @param familyName 列族名
   */
  public void cleanFamily(String tableName, String rowKye, String familyName) throws IOException {
    Table table = hBaseResources.getConnection().getTable(TableName.valueOf(tableName));
    Delete delete = new Delete(Bytes.toBytes(rowKye));
    delete.addFamily(Bytes.toBytes(familyName));
    table.delete(delete);
  }

  /**
   * 删除列族
   *
   * @param tableName 表名
   * @param familyName 列族名
   */
  public void deleteFamily(String tableName, String familyName) throws IOException {
    Admin admin = hBaseResources.getConnection().getAdmin();
    admin.deleteColumn(TableName.valueOf(tableName), Bytes.toBytes(familyName));
  }

  /**
   * 获取指定rowkey，指定family的所有列
   *
   * @param tableName 表名
   * @param familyName 列族
   * @param rowKye rowkey
   */
  public void columnList(String tableName, String familyName, String rowKye) throws IOException {
    Table table = hBaseResources.getConnection().getTable(TableName.valueOf(tableName));
    Get get = new Get(Bytes.toBytes(rowKye));
    get.addFamily(Bytes.toBytes(familyName));
    Result result = table.get(get);
    if (result.rawCells().length > 0) {
      List<Cell> listCell = result.listCells();
      for (Cell cell : listCell) {
        String cellName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
        System.out.println("qualifier = " + cellName);
      }
    }
  }
}
