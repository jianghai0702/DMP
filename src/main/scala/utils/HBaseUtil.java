package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseUtil {

    private static Configuration configuration;

    static {
        //创建配置对象，指定zk集群地址
        //郑重配置加载方式会加载default-site.xml
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","node22:2181,node23:2181,node24:2181");
    }

    public static Admin getAdmin() throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        return admin;
    }

    public static void close(Admin admin) throws IOException {
        admin.close();
    }
}
