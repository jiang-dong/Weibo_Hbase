import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseVersionAndTTL {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop01:2181,hadoop02:2181,hadoop03:2181");
        Connection connection = ConnectionFactory.createConnection();
        Admin admin = connection.getAdmin();
        if(!admin.tableExists(TableName.valueOf("version_hbase"))){
            TableDescriptorBuilder version_hbase = TableDescriptorBuilder.newBuilder(TableName.valueOf("version_hbase"));
            //添加列簇
            ColumnFamilyDescriptor family_base_info = ColumnFamilyDescriptorBuilder.newBuilder("base_info".getBytes()).build();


            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("version_hbase"));
            HColumnDescriptor f1 = new HColumnDescriptor("f1");


            f1.setMinVersions(3);
            f1.setMaxVersions(5);
            //针对某一个列族下面所有的列设置TTL
            f1.setTimeToLive(30);
            hTableDescriptor.addFamily(f1);
            admin.createTable(hTableDescriptor);
        }
        Table version_hbase = connection.getTable(TableName.valueOf("version_hbase"));
//        Put put = new Put("1".getBytes());
//        //针对某一条具体的数据设置TTL
//        //put.setTTL(3000);
//        put.addColumn("f1".getBytes(),"name".getBytes(),System.currentTimeMillis(),"zhangsan".getBytes());
//        version_hbase.put(put);
//        Thread.sleep(1000);
//
//        Put put2 = new Put("1".getBytes());
//        put2.addColumn("f1".getBytes(),"name".getBytes(),System.currentTimeMillis(),"zhangsan2".getBytes());
//        version_hbase.put(put2);
//
//        Put put3 = new Put("1".getBytes());
//        put3.addColumn("f1".getBytes(),"name".getBytes(),System.currentTimeMillis(),"zhangsan3".getBytes());
//        version_hbase.put(put3);
//
//        Put put4 = new Put("1".getBytes());
//        put4.addColumn("f1".getBytes(),"name".getBytes(),System.currentTimeMillis(),"zhangsan4".getBytes());
//        version_hbase.put(put4);
        //查看数据
        Get get = new Get("1".getBytes());
        get.setMaxVersions();

        Result result = version_hbase.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
        version_hbase.close();
        connection.close();
    }
}
