import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import javax.swing.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WeiboHbase {
    //微博内容表
    private String TABLE_CONTENT = "weibo:content";
    // 用户关系表
    private String TABLE_USER_RELATION = "weibo:relation";
    //微博推送表
    private String TABLE_RECEIVE_EMAIL = "weibo:receive_mail";


    public static void main(String[] args) throws IOException {
        WeiboHbase weiboHbase = new WeiboHbase();

        //weiboHbase.initNameSpace();
        //weiboHbase.createContent();
        //weiboHbase.createUserRelation();
        // weiboHbase.createReceiveEmail();
    }

    //获取连接
    private Connection getConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    /**
     * 创建命名空间
     */
    public void initNameSpace() throws IOException {
        //获取空间
        Connection connection = getConnection();
        ////获取admin
        Admin admin = connection.getAdmin();
        //创建命名空间
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("weibo").build();
        admin.createNamespace(namespaceDescriptor);

        //关闭资源
        admin.close();
        connection.close();
    }

    /**
     * 创建内容表
     */

    public void createContent() throws IOException {
        // 连接
        Connection connection = getConnection();
        //获取admin对象
        Admin admin = connection.getAdmin();
        //判断是否存在,不存在则创建
        if (!admin.tableExists(TableName.valueOf(TABLE_CONTENT))) {
            ColumnFamilyDescriptor info = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes()).setMaxVersions(1).setMinVersions(1).build();
            // ColumnFamilyDescriptor attens = ColumnFamilyDescriptorBuilder.newBuilder("attens".getBytes()).setMinVersions(1).setMaxVersions(1).build();

            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_CONTENT))
                    .setColumnFamily(info)
                    .build();
            admin.createTable(tableDescriptor);

        }
        admin.close();
        connection.close();
    }


    /**
     * 创建用户关系表
     */

    public void createUserRelation() throws IOException {
        // 连接
        Connection connection = getConnection();
        //获取admin对象
        Admin admin = connection.getAdmin();
        //判断是否存在,不存在则创建
        if (!admin.tableExists(TableName.valueOf(TABLE_USER_RELATION))) {
            ColumnFamilyDescriptor fans = ColumnFamilyDescriptorBuilder.newBuilder("fans".getBytes()).setMaxVersions(1).setMinVersions(1).build();
            ColumnFamilyDescriptor attens = ColumnFamilyDescriptorBuilder.newBuilder("attens".getBytes()).setMinVersions(1).setMaxVersions(1).build();

            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_USER_RELATION))
                    .setColumnFamily(fans)
                    .setColumnFamily(attens)
                    .build();
            admin.createTable(tableDescriptor);

        }
        admin.close();
        connection.close();
    }


    /**
     * 创建微博推送表
     */

    public void createReceiveEmail() throws IOException {
        // 连接
        Connection connection = getConnection();
        //获取admin对象
        Admin admin = connection.getAdmin();
        //判断是否存在,不存在则创建
        if (!admin.tableExists(TableName.valueOf(TABLE_RECEIVE_EMAIL))) {
            ColumnFamilyDescriptor info = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes()).setMaxVersions(1000).setMinVersions(1000).build();
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_RECEIVE_EMAIL))
                    .setColumnFamily(info)
                    .build();
            admin.createTable(tableDescriptor);

        }
        admin.close();
        connection.close();
    }


    /**
     * 发布微博内容
     * 第一步: 需要将微博内容保存到content 表中
     * 第二步: 将A的粉丝需要查看到A发布的内容,需要查看A用户有哪些粉丝,查询relation关系表,fans和attes列簇,有那些粉丝
     * 第三步: 需要给粉丝添加A用户微博的rowkey ,在receive_content_email 表当中以fans的ID作为rowkey,然后以用户
     * UID为列名,用户微博的rowkey作为列值
     *
     */

    /**
     * a、微博内容表中添加1条数据
     * b、微博收件箱表对所有粉丝用户添加数据
     */
    public void creatTableeContent(String uid, String content) throws IOException {
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        String rowkey = uid + "_" + System.currentTimeMillis();
        Put put = new Put(rowkey.getBytes());
        put.addColumn("info".getBytes(), "content".getBytes(), System.currentTimeMillis(), content.getBytes());
        table.put(put);

        //微博用户关系表
        Table table_relation = connection.getTable(TableName.valueOf(TABLE_USER_RELATION));
        Get get = new Get(uid.getBytes());
        get.addFamily("fans".getBytes());
        Result result = table_relation.get(get);
        Cell[] cells = result.rawCells();
        if (cells.length <= 0) {
            return;
        }
        //将所有的粉丝都获取并将数据保存到粉丝表中
        List<byte[]> allFans = new ArrayList<>();
        for (Cell cell : cells) {
            byte[] bytes = CellUtil.cloneQualifier(cell);
            allFans.add(bytes);
        }
        Table table_receive_content_email = connection.getTable(TableName.valueOf(TABLE_RECEIVE_EMAIL));
        List<Put> putFansList = new ArrayList<>();
        for (byte[] allFan : allFans) {
            Put put1 = new Put(allFan);
            put1.addColumn("info".getBytes(), Bytes.toBytes(uid), System.currentTimeMillis(), rowkey.getBytes());
            putFansList.add(put1);
        }
        table_receive_content_email.put(putFansList);
    }


    /**
     * 添加关注用户:
     * 例如A 用户关注了B,C,D和三个用户,A用户是B,C,D的粉丝,B,C,D的粉丝是A的用户
     * A用户需要查看B,C,D发送的微博内容
     * <p>
     * 难题:知道三个用户的UID,如何通过UID来查询weibo:content表里面三个用户 发送的微博内容的rowkey
     */
    public void addAttends(String uid, String... attends) throws IOException {
        Connection connection = getConnection();
        Table relation_table = connection.getTable(TableName.valueOf(TABLE_USER_RELATION));
        //用户关注人,attend列簇中添加数据
        Put put = new Put(uid.getBytes());
        for (String attend : attends) {
            put.addColumn("attends".getBytes(), attend.getBytes(), attend.getBytes());
        }
        relation_table.put(put);
        //粉丝fans添加fans
        for (String attend : attends) {
            Put put1 = new Put((attend.getBytes()));
            put1.addColumn("fans".getBytes(), uid.getBytes(), uid.getBytes());
            relation_table.put(put1);
        }

        //获取uid的所有关注人的收件箱,放到收件箱列表weibo:recerive_content_email
        //A 关注B  ,那么A需要所获取B的所有微博内容
        Table table_content = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        Scan scan = new Scan();
        List<byte[]> rowkeyBytes = new ArrayList<>();
        for (String attend : attends) {
            String prefix =attend +"_";
            PrefixFilter prefixFilter = new PrefixFilter("prefix".getBytes());
            scan.setFilter(prefixFilter);
            ResultScanner scanner = table_content.getScanner(scan);
            for (Result result : scanner) {
                //获取的数据rowkey
                byte[] rowkey = result.getRow();
                rowkeyBytes.add(rowkey);
            }
        }

        if (rowkeyBytes.size()>0){
            Table table_receive_content = connection.getTable(TableName.valueOf(TABLE_RECEIVE_EMAIL));
            List<Put> recPuts = new ArrayList<>();

            for (byte[] rowkeyByte : rowkeyBytes) {
                Put put1 = new Put(uid.getBytes());
                String rowKeyStr = Bytes.toString(rowkeyByte);
                String attendUid = rowKeyStr.substring(0, rowKeyStr.indexOf("_"));
                Long timestamp =Long.parseLong(rowKeyStr.substring(rowKeyStr.indexOf("_"+1)));
                put1.addColumn("info".getBytes(), attendUid.getBytes(), timestamp, rowkeyByte);
                recPuts.add(put1);
            }
        }
    }


    /**
     * 取消关注:
     * 第一步: 在weibo:relation表中取消A用户关注的B,C,D
     * 第二步:BCD三个人会少一个粉丝 ,  fans列簇里面少一个数据A
     * 第三步: 在微博的收件表中, 上传关注BCD的微博Rowkey
     *
     *a、在微博用户关系表中，对当前主动操作的用户移除取关的好友(attends)
     * b、在微博用户关系表中，对被取关的用户移除粉丝
     * c、微博收件箱中删除取关的用户发布的微博
     *
     */

    public void deleteAttends(String uid, String... deleteAttends) throws IOException {
        Connection connection = getConnection();
        Table relation_table = connection.getTable(TableName.valueOf(TABLE_USER_RELATION));
        //移除A关注的BCD对象三个用户
        Delete delete = new Delete(uid.getBytes());
        for (String deleteAttend : deleteAttends) {
            delete.addColumn("attends".getBytes(), deleteAttend.getBytes());
            relation_table.delete(delete);
        }

        //粉丝fans  BCD 三个用户 移除 添加 A
        for (String deleteAttend : deleteAttends) {
            Delete delete1 = new Delete(deleteAttend.getBytes());
            delete1.addColumn("fans".getBytes(), uid.getBytes());
            relation_table.delete(delete1);
        }

        //获取uid的所有关注人的收件箱,放到收件箱列表weibo:recerive_content_email
        //A 关注B  ,那么A需要所获取B的所有微博内容
        Table table_receive_email = connection.getTable(TableName.valueOf(TABLE_RECEIVE_EMAIL));
        for (String deleteAttend : deleteAttends) {
            Delete delete2 = new Delete(deleteAttend.getBytes());
            delete2.addColumn("fans".getBytes(), uid.getBytes());
            table_receive_email.delete(delete2);
        }

        table_receive_email.close();
        relation_table.close();
        connection.close();
    }






}

