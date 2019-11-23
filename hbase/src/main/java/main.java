import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class main {
    private static void insert(Table table,String id,String columnfamily,String qualifier,String value) throws IOException {
            byte[] row = Bytes.toBytes(id); //定义行
            Put put = new Put(row);             //创建Put对象
            byte[] c = Bytes.toBytes(columnfamily);    //列
            byte[] q = Bytes.toBytes(qualifier); //列族修饰词
            byte[] v = Bytes.toBytes(value);    //值
            put.addColumn(c, q, v);
            table.put(put);     //向表中添加数据
    }

    public static void main(String[] args) throws IOException {
        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.zookeeper.quorum","h0,h1");
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort","2181");

        Configuration config=HBaseConfiguration.create(HBASE_CONFIG);
        Connection conn = ConnectionFactory.createConnection(config);

        Admin admin=conn.getAdmin();
        TableName tableName=TableName.valueOf("students");
        TableDescriptorBuilder tabledescriptor=TableDescriptorBuilder.newBuilder(tableName);
        ArrayList<ColumnFamilyDescriptor> families=new ArrayList<ColumnFamilyDescriptor>();
        families.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("Description")).build());
        families.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("Courses")).build());
        families.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("Home")).build());
        tabledescriptor.setColumnFamilies(families);
        admin.createTable(tabledescriptor.build());

        Table table=conn.getTable(tableName);
        try {
            insert(table, "001", "Description", "Name", "Li Lei");
            insert(table, "001", "Description", "Height", "176");
            insert(table, "001", "Courses", "Chinese", "80");
            insert(table, "001", "Courses", "Math", "90");
            insert(table, "001", "Courses", "Physics", "95");
            insert(table, "001", "Home", "Province", "Zhejiang");
            insert(table, "002", "Description", "Name", "Han Meimei");
            insert(table, "002", "Description", "Height", "183");
            insert(table, "002", "Courses", "Chinese", "88");
            insert(table, "002", "Courses", "Math", "77");
            insert(table, "002", "Courses", "Physics", "66");
            insert(table, "002", "Home", "Province", "Beijing");
            insert(table, "003", "Description", "Name", "Xiao Ming");
            insert(table, "003", "Description", "Height", "162");
            insert(table, "003", "Courses", "Chinese", "90");
            insert(table, "003", "Courses", "Math", "90");
            insert(table, "003", "Courses", "Physics", "90");
            insert(table, "003", "Home", "Province", "Shanghai");
        }finally {
            table.close();
        }
    }
}
