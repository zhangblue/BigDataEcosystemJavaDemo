package zhangblue.hbase.demo;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import zhangblue.hbase.repository.HBaseResources;

public class ExampleDemo {

  private HBaseResources hBaseResources;

  public ExampleDemo(HBaseResources hBaseResources) {
    this.hBaseResources = hBaseResources;
  }


  public void getByRowKey(String tableName, String family, String rowKey) {

    try {
      Table table = hBaseResources.getConnection().getTable(TableName.valueOf(tableName));
      Get get = new Get(Bytes.toBytes(rowKey));

      Result result = table.get(get);
      for (Cell cell : result.listCells()) {

        System.out.println("family = " +
            Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));

        System.out.println("qualifier = " + Bytes
            .toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                cell.getQualifierLength()));

        System.out.println("value = "+Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
