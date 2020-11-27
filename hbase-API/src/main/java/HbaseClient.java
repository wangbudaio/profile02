import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 王继昌
 * @create 2020-10-12 10:03
 */
public class HbaseClient {
    /*
    *获取连接
    * */

    private static HbaseClient hbaseClient;
    private Connection connection ;


    private Connection conntects(Connection connection){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        try {
            connection = ConnectionFactory.createConnection(conf);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            return connection;
        }
    }

    public Connection HbaseClient() {
        if (connection != null){
            return connection;
        }
        return conntects(connection);
    }

    public static HbaseClient getConnect(){
        if (hbaseClient != null) {
            return hbaseClient;
        }
        return new HbaseClient();
    }

    /*
    * namespace的创建
    * */
    @Test
    public void test(){
        HbaseClient hbaseClient = HbaseClient.getConnect();
        Connection connection = hbaseClient.HbaseClient();
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("atguigu");
            NamespaceDescriptor descriptor = builder.build();
            admin.createNamespace(descriptor);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
        }
    }

    /*
    * namespace的删除
    * */
    @Test
    public void test1(){
        HbaseClient hbaseClient = HbaseClient.getConnect();
        Connection connection = hbaseClient.HbaseClient();
        Admin admin = null;
        try {
            //获取Admin对象
            admin = connection.getAdmin();
            //创建Builder对象
            NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("atguigu");
            //创建NamespaceDescriptor对象
            NamespaceDescriptor descriptor = builder.build();
            //添加命名空间
            admin.createNamespace(descriptor);
            //删除命名空间
            //        admin.deleteNamespace("atguigu");
            //关闭admin
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
        }
    }

    /*
     * table的创建
     * */
    @Test
    public void test2(){
        HbaseClient hbaseClient = HbaseClient.getConnect();
        Connection connection = hbaseClient.HbaseClient();
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(Bytes.toBytes("atguigu"), Bytes.toBytes("teacher"));
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            byte[] info = Bytes.toBytes("info");
            ColumnFamilyDescriptorBuilder familyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(info);
            ColumnFamilyDescriptor columnFamilyDescriptor  = familyDescriptorBuilder.build();
            TableDescriptorBuilder builder1 = builder.setColumnFamily(columnFamilyDescriptor);
            TableDescriptor build = builder.build();
            admin.createTable(build);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
        }
    }


    /*
     * table的删除
     * */
    @Test
    public void test3(){
        HbaseClient hbaseClient = HbaseClient.getConnect();
        Connection connection = hbaseClient.HbaseClient();
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(Bytes.toBytes("atguigu"), Bytes.toBytes("teacher"));
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
        }
    }

    /*
     * table插入数据
     * */
    @Test
    public void test4(){
        HbaseClient hbaseClient = HbaseClient.getConnect();
        Connection connection = hbaseClient.HbaseClient();
        Table table = null;
        try {
            TableName tableName = TableName.valueOf(Bytes.toBytes("student"));
            table = connection.getTable(tableName);
            /*Put put = new Put(Bytes.toBytes("1001"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("张三"));
            Put put1 = new Put(Bytes.toBytes("1001"));
            put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(18));
            List<Put> puts = new ArrayList<>();
            */
            Put put = new Put(Bytes.toBytes("1001"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("张三"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(18));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
        }
    }


    /*
     * 查看get
     * */
    @Test
    public void test5(){
        HbaseClient hbaseClient = HbaseClient.getConnect();
        Connection connection = hbaseClient.HbaseClient();
        Table table = null;
        try {
            TableName tableName = TableName.valueOf(Bytes.toBytes("atguigu"),Bytes.toBytes("teacher"));
            table = connection.getTable(tableName);
            Get get = new Get(Bytes.toBytes("1001"));
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                byte[] rowKey  = CellUtil.cloneRow(cell);
                byte[] family  = CellUtil.cloneFamily(cell);
                byte[] qualifier  = CellUtil.cloneQualifier(cell);
                byte[] value  = CellUtil.cloneValue(cell);
                System.out.println("RowKey是："+Bytes.toString(rowKey));
                System.out.println("列族是："+Bytes.toString(family));
                System.out.println("列名是："+Bytes.toString(qualifier));
                System.out.println("列值是："+Bytes.toString(value));
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
        }
    }

    /*
     * 查看scan
     * */
    @Test
    public void test6(){
        HbaseClient hbaseClient = HbaseClient.getConnect();
        Connection connection = hbaseClient.HbaseClient();
        Table table = null;
        try {
            TableName tableName = TableName.valueOf(Bytes.toBytes("atguigu"),Bytes.toBytes("teacher"));
            table = connection.getTable(tableName);
//            Scan scan = new Scan(Bytes.toBytes("1001"));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes("1001"));
            scan.withStopRow(Bytes.toBytes("1002"));
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    byte[] rowKey  = CellUtil.cloneRow(cell);
                    byte[] family  = CellUtil.cloneFamily(cell);
                    byte[] qualifier  = CellUtil.cloneQualifier(cell);
                    byte[] value  = CellUtil.cloneValue(cell);
                    System.out.println("RowKey是："+Bytes.toString(rowKey));
                    System.out.println("列族是："+Bytes.toString(family));
                    System.out.println("列名是："+Bytes.toString(qualifier));
                    System.out.println("列值是："+Bytes.toString(value));
                }
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
        }
    }

    /*
     * 删除数据
     * */

    @Test
    public void test7(){


        HbaseClient hbaseClient = HbaseClient.getConnect();
        Connection connection = hbaseClient.HbaseClient();
        Table table = null;
        try {
            TableName tableName = TableName.valueOf(Bytes.toBytes("atguigu"),Bytes.toBytes("teacher"));
            table = connection.getTable(tableName);
//            Delete delete = new Delete(Bytes.toBytes("1001"));
//            delete.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("age"));
//            table.delete(delete);
            Delete delete = new Delete(Bytes.toBytes("1001"));
            table.delete(delete);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
        }
    }

}

