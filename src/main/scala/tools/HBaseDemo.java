package tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import utils.HBaseUtil;

import java.io.IOException;

public class HBaseDemo {

    private static Admin admin = null;
    static {
        try {
            admin = HBaseUtil.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * 连接测试
     */
    @Test
    public void test_connect() throws IOException {
        //创建配置对象，指定zk集群地址
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","node22:2181,node23:2181,node24:2181");

        //获取连接对象
        HBaseAdmin admin = new HBaseAdmin(configuration);
        boolean tableExists = admin.tableExists("ns1:t1");
        System.out.println(tableExists);
        admin.close();
    }


    /**
     * 命名空间
     */
    @Test
    public void test_namespace() throws IOException {
        String dbname = "db01";
        NamespaceDescriptor db01 = NamespaceDescriptor.create(dbname).build();
        db01.setConfiguration("name", "monkey1024");
        admin.deleteNamespace(dbname);
        admin.createNamespace(db01);
        print_namespaces();
    }

    //打印namespace
    public void print_namespaces() throws IOException {
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor ns : namespaceDescriptors) {
            System.out.println("namespace : " + ns.getName());
        }
    }



    @Test
    public void test_table() throws IOException {
        String table = "db01:user";
        TableName tableName = TableName.valueOf(table);
        if (admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        HTableDescriptor table_user = new HTableDescriptor(table);
        HColumnDescriptor family_base = new HColumnDescriptor("family_base");
        table_user.addFamily(family_base); // 一个表至少有一个列族
        admin.createTable(table_user);

        printTables("db01");
    }
    // 打印所有表
    public void printTables(String namespace) throws IOException {
        TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
        for (TableName tableName : tableNames) {
            System.out.println(tableName.getNameAsString());
        }
    }



    @Test
    public void test_curd() throws IOException {
        Table table = admin.getConnection().getTable(TableName.valueOf("db01:user"));
        //
        Put put = new Put(Bytes.toBytes(""));
        table.put(put);
    }



}
