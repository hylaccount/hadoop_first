package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by Administrator on 2017/3/22.
 */
public class SecondarySortMapReduce {
     /*
     writable是HDFS的序列化接口
     数据都是以<key value>传递
     * */
    public static class SecondarySortMapper extends Mapper<LongWritable,Text,PairWritable,IntWritable>{
         private PairWritable mapOutKey =new PairWritable();
         private  IntWritable mapOutValue=new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line =value.toString();
            //把每一行的内容按","切分
            String strs[]=line.split(" ");
            mapOutKey.set(strs[0], Integer.valueOf(strs[1]));
            mapOutValue.set(Integer.valueOf(strs[1]));
            context.write(mapOutKey,mapOutValue);
            
        }
    }
/*
* shuffle 这是一个阶段,分组的作用,把map整理好的相同的key放到一个组里面,将对应的每个value放到一个集合里面,
* <key,list[1,1,1,1,...]>
* 便于reducer阶段的统计
* */
    public static class SecondarySortReduce extends Reducer<PairWritable,IntWritable,Text,IntWritable>{
      
    	
    	private Text outputkey=new Text();
    	@Override
        protected void reduce(PairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           for(IntWritable value :values){
        	   outputkey.set(key.getFirst());
        	   context.write(outputkey, value);
           }
        }
    }
    public int run(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.实例化一个configuration类,获取hadoop的配置信息
        Configuration config=new Configuration();
        //2.生成job
        Job job=Job.getInstance( config,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        //3.设置job的内容
        //input -> map ->reduce ->output
        Path inPath = new Path(args[0]);//输入路径
        FileInputFormat.setInputPaths(job,inPath);

        //3.1:map
        job.setMapperClass(SecondarySortMapper.class);
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        //3.2:reduce
        job.setReducerClass(SecondarySortReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //3.4:输出
        Path outPath=new Path(args[1]);
        //输出目录如果存在,自动删除
        FileSystem fs=FileSystem.get(config);
        FileSystem fhs=outPath.getFileSystem(config);
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);

        //4.提交的job运行是否成功
        boolean isSuccess =job.waitForCompletion(true);
        return isSuccess ? 0:1;//成功是0,失败是1
    }
    public static void main(String []args) throws InterruptedException, IOException, ClassNotFoundException {
        args =new String []{
                "hdfs://ns1/input/sort.txt",
                "hdfs://ns1/output3"
        };
        int status=new SecondarySortMapReduce().run(args);
        System.exit(status);
    }
}
