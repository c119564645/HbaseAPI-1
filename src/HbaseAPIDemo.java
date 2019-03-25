import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HbaseAPIDemo {

    @Test
    public void CreateTable() throws Exception {
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum","192.168.136.128:2181,192.168.136.129:2181,192.168.136.130:2181");

        HBaseAdmin admin = new HBaseAdmin(conf);
        //指定表名
        HTableDescriptor cwj1 = new HTableDescriptor(TableName.valueOf("cwj1"));
        //指定列族名
        HColumnDescriptor colfam1=new HColumnDescriptor("colfam1".getBytes());
        HColumnDescriptor colfam2=new HColumnDescriptor("colfam2".getBytes());

        //指定历史版本存留上限
        colfam1.setMaxVersions(3);
        cwj1.addFamily(colfam1);
        cwj1.addFamily(colfam2);
        //创建表
        admin.createTable(cwj1);
        admin.close();
    }

    @Test
    public void Insert()throws Exception{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.136.128:2181,192.168.136.129:2181,192.168.136.130:2181");
        //尽量复用Htable对象
        HTable table = new HTable(conf, "cwj1");
        Put put = new Put("row-1".getBytes());
        //列族,列,值
        put.add("colfam1".getBytes(),"col1".getBytes(),"aaa".getBytes());
        put.add("colfam1".getBytes(),"col2".getBytes(),"bbb".getBytes());
        table.put(put);
        table.close();
    }


    @Test
    public void InsertMillion() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.136.128:2181,192.168.136.129:2181,192.168.136.130:2181");

        HTable table = new HTable(conf, "cwj1");
        List<Put> puts = new ArrayList<>();
        long begin = System.currentTimeMillis();

        for (int i = 1; i < 1000000; i++) {
            Put put = new Put(("row" + i).getBytes());
            put.add("colfam1".getBytes(),"col".getBytes(),(""+i).getBytes());
            puts.add(put);

            //批处理,批大小为10000,每到一万提交一次
            if(i%10000==0){
                table.put(puts);
                puts=new ArrayList<>();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(end-begin);
    }

    @Test
    public void Get()throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.136.128:2181,192.168.136.129:2181,192.168.136.130:2181");
        HTable table=new HTable(conf,"cwj1");
        Get get = new Get("row1".getBytes());
        Result result = table.get(get);
        byte[] col1_result = result.getValue("colfam1".getBytes(), "col".getBytes());

        System.out.println(col1_result.toString());
        table.close();

    }

    @Test
    public void Scan()throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.136.128:2181,192.168.136.129:2181,192.168.136.130:2181");
        HTable table=new HTable(conf,"cwj1");
        //获取row100以及以后的行键的值
        Scan scan = new Scan("row100".getBytes());
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> it = scanner.iterator();
        while(it.hasNext()){
            Result result = it.next();
            byte[] bs = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("col"));
            String srt = Bytes.toString(bs);
            System.out.println(srt);
        }

    }
    @Test
    public void Delete() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.136.128:2181,192.168.136.129:2181,192.168.136.130:2181");
        HTable table = new HTable(conf, "cwj1");
        Delete delete = new Delete("row1".getBytes());
        table.delete(delete);
        table.close();
    }

    @Test
    public void Drop() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.136.128:2181,192.168.136.129:2181,192.168.136.130:2181");

        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable("cwj1".getBytes());
        admin.deleteTable("cwj1".getBytes());
        System.out.println("Delete Ok!");
        admin.close();
    }








}
