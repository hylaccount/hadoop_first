package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 * Created by Administrator on 2017/4/12.
 */
public class Hbase_First {
    public static void main(String[] args) throws IOException {
          Configuration conf= HBaseConfiguration.create();
        HBaseAdmin admin =new HBaseAdmin(conf);
        //create HtableDescriptor and set table name
        HTableDescriptor hTableDescriptor=new HTableDescriptor(TableName.valueOf("tb_java"));
        //设置列簇名
        HColumnDescriptor hColumn=new HColumnDescriptor("info");
        //设置历史版本
        hColumn.setValue("VERSIONS","5");
        //把设置好的列簇名添加给表
        hTableDescriptor.addFamily(hColumn);
        admin.createTable(hTableDescriptor);
        admin.close();


    }
    public static HTable getTable(String tb_name) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table= gethTable(tb_name, conf);
        return table;
    }

    private static HTable gethTable(String tb_name, Configuration conf) throws IOException {
        return new HTable(conf,tb_name);
    }
}
