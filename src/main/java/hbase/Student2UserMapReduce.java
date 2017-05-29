package hbase;


import com.sun.beans.editors.ByteEditor;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by Administrator on 2017/4/13.
 */
public class Student2UserMapReduce extends Configured implements Tool {


    public  static class  Hb2HbMapper extends TableMapper<ImmutableBytesWritable,Put>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Put put =new Put(key.get());
            for(Cell cell :value.rawCells()){
                if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                    if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                        put.add(Bytes.toBytes("basic"), Bytes.toBytes("name1"),CellUtil.cloneValue(cell));
                    }

                }
                context.write(key,put);
            }

        }
    }





    public int run(String[] strings) throws Exception {
        return 0;
    }
}
