package zhangblue.hbase.repository;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseResources {

  private Configuration hBaseConfiguration;
  private Connection connection;

  public void initHBaseConfig() {
    hBaseConfiguration = HBaseConfiguration.create();
    hBaseConfiguration.addResource(this.getClass().getResourceAsStream("/hbase-site.xml"));
  }

  public void initConnection() throws IOException {
    connection = ConnectionFactory.createConnection(hBaseConfiguration);
  }

  public void closeConnection() throws IOException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  public Configuration gethBaseConfiguration() {
    return hBaseConfiguration;
  }


  public Connection getConnection() {
    return connection;
  }
}
