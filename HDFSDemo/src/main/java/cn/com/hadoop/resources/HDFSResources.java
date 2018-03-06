package cn.com.hadoop.resources;

import org.apache.hadoop.conf.Configuration;

public class HDFSResources {


  private Configuration hadoopConfig;

  public void initHDFSConfig() {
    hadoopConfig = new Configuration();
    hadoopConfig.addResource(this.getClass().getResourceAsStream("/core-site.xml"));
  }

  public Configuration getHadoopConfig() {
    return hadoopConfig;
  }
}
