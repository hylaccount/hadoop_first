package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Administrator on 2017/5/24.
 */
public class WordCountTest {
    public static class wordcountmap extends Mapper<IntWritable ,Text,Text,IntWritable>{
        private Text mapkey=new Text();
        private IntWritable outputval=new IntWritable(1);
        protected void map(IntWritable key ,Text value ,Context context) throws IOException, InterruptedException {
            String line =value.toString();
            String li[]=line.split(",");
            for (String str:li){
                mapkey.set(str);
                context.write(mapkey,outputval);
            }
        }
    }
    public static class wordcountreduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable value:values){
                sum+=value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
    public int run(String [] args) throws IOException {
        Configuration config=new Configuration();
        Job job=Job.getInstance(config,this.getClass().getSimpleName());




        return 0;
    }


}
