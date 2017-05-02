package TEST;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class hbase_test1 {
    private static final String TABLE_NAME = "test_TimeSeqData";
    private static final String COLUMN_FAMILY_NAME = "d1";

    static Configuration cfg=HBaseConfiguration.create();

    public static void creat(String tablename,String columnFamily) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tablename)) {
            System.out.println("table Exists!");
            System.exit(0);
        }
        else{
            HTableDescriptor tableDesc = new HTableDescriptor(tablename);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println("create table success!");
        }
    }

    public static void put(String tablename,String row, String columnFamily,String column,String data) throws Exception {
        HTable table = new HTable(cfg, tablename);
        Put p1=new Put(Bytes.toBytes(row));
        p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
        table.put(p1);
        System.out.println("put '"+row+"','"+columnFamily+":"+column+"','"+data+"'");
    }

    public static void get(String tablename,String row) throws IOException{
        HTable table=new HTable(cfg,tablename);
        Get g=new Get(Bytes.toBytes(row));
        Result result=table.get(g);
        System.out.println("Get: "+result);
    }
    //显示所有数据，通过HTable Scan来获取已有表的信息
    public static void scan(String tablename) throws Exception{
        HTable table = new HTable(cfg, tablename);
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);
        for(Result r:rs){
            System.out.println("Scan: "+r);
        }
    }

    public static boolean delete(String tablename) throws IOException{

        HBaseAdmin admin=new HBaseAdmin(cfg);
        if(admin.tableExists(tablename)){
            try
            {
                admin.disableTable(tablename);
                admin.deleteTable(tablename);
            }catch(Exception ex){
                ex.printStackTrace();
                return false;
            }

        }
        return true;
    }

    public static void  main (String [] agrs) {
    /*    hbase_test1 aa = new hbase_test1();
        try {
            File file = new File("/tmp/test.txt");
            BufferedReader reader = null;

            //int i = 0;

            reader = new BufferedReader(new FileReader(file));
            String tempString = null;

            long startTime = System.currentTimeMillis();

            while ((tempString = reader.readLine()) != null) {

                String [] arr =  tempString.split("\\s+");
                String pTime = arr[0] + " " + arr[1];
                String pData = arr[2];
                aa.put("test_TimeSeqData",pTime,"d1","p1",pData);
            }
            reader.close();

            long endTime = System.currentTimeMillis();
            System.out.println("program run time:"+(endTime-startTime)+"ms");
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }*/

        hbase_test1 manageMain = new hbase_test1();
        try {
            cfg.set("fs.hdfs.impl",
                    org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
            );
            cfg.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName()
            );
            Connection connection = ConnectionFactory.createConnection(cfg);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            byte[] family = Bytes.toBytes(COLUMN_FAMILY_NAME);
            Table table = connection.getTable(tableName);

            File file = new File("/tmp/test.txt");
            BufferedReader reader = null;
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;

            long startTime = System.currentTimeMillis();

            while ((tempString = reader.readLine()) != null) {

                String[] arr = tempString.split("\\s+");
                String pTime = arr[0] + " " + arr[1];
                String pData = arr[2];

                byte[] rowkey = Bytes.toBytes(pTime);
                //byte[] rowkey = pTime.getBytes();
                Put put = new Put(rowkey);
                byte[] qualifier = Bytes.toBytes("p1");
                //byte[] qualifier = "p1".getBytes();
                byte[] value = Bytes.toBytes(pData);
                //byte[] value = pData.getBytes();
                put.addColumn(family, qualifier, value);

                table.put(put);


                // aa.put("test_TimeSeqData",pTime,"d1","p1",pData);
            }

            reader.close();

            long endTime = System.currentTimeMillis();
            System.out.println("program run time:" + (endTime - startTime) + "ms");

            //manageMain.putDatas(connection);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
