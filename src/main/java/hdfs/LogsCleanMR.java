package hdfs;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogsCleanMR extends Configured implements Tool {

	// step 1: Mapper Class
	public static class WebPvMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

//		private IntWritable mapOutputValue = new IntWritable(1);
//		private IntWritable mapOutputKey = new IntWritable();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// get line value
			String lineValue = value.toString();

			// split
			String[] values = lineValue.split("\t");

			if (30 > values.length) {
				// counter
				context.getCounter("WEBPVMAPPER_COUNTER",
						"LENGTH_LT_30_COUNTER").increment(1L);

				return;
			}

			// url
//			String urlValue = values[1];
//			if (StringUtils.isBlank(urlValue)) {
//				// counter
//				context.getCounter("WEBPVMAPPER_COUNTER", "URL_BLANK_COUNTER")
//						.increment(1L);
//				return;
//			}

			// province ID
			String provinceIdValue = values[23];

			if (StringUtils.isBlank(provinceIdValue)) {
				// counter
				context.getCounter("WEBPVMAPPER_COUNTER",
						"PROVINCEID_BLANK_COUNTER").increment(1L);
				return;
			}

			Integer provinceId = Integer.MAX_VALUE;

			try {
				provinceId = Integer.valueOf(provinceIdValue);
			} catch (Exception e) {
				// counter
				context.getCounter("WEBPVMAPPER_COUNTER",
						"PROVINCEID_NOT_NUMBER_COUNTER").increment(1L);
				return;
			}

			// map output key
//			mapOutputKey.set(provinceId);

//			context.write(mapOutputKey, mapOutputValue);
			context.write(key,value);

		}

	}

	// step 2: Reduce Class
	public static class WebPvReducer extends
			Reducer<LongWritable, Text, Text, NullWritable> {

		//private IntWritable outputValue = new IntWritable();

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			// tmp
			int sum = 0;

			// iterator
			for (Text value : values) {
				// total
				//sum += value.get();
				context.write(value,NullWritable.get());
			}
			// set
			//outputValue.set(sum);

			// output
			//context.write(key, outputValue);
		}

	}

	/**
	 * /** Execute the command with the given arguments.
	 * 
	 * @param args
	 *            command specific arguments.
	 * @return exit code.
	 * @throws Exception
	 *             int run(String [] args) throws Exception;
	 */

	// step 3: Driver
	public int run(String[] args) throws Exception {

		Configuration configuration = this.getConf();
		Job job = Job.getInstance(configuration, this.getClass()
				.getSimpleName());
		job.setJarByClass(this.getClass());

		// set job
		// input
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);

		// mapper
		job.setMapperClass(WebPvMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		// reducer
		job.setReducerClass(WebPvReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		//output
		Path outPath = new Path(args[1]);
		
		FileSystem dfs = FileSystem.get(configuration);
		FileSystem dfsh = outPath.getFileSystem(configuration);
		if (dfsh.exists(outPath)) {
			dfsh.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);
		
		// submit job
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		Configuration configuration = new Configuration();

		args = new String[] {
				"hdfs://ibeifeng3.com:8020/input/2015082818",
				"hdfs://ibeifeng3.com:8020/output"
//				"D:\\2015082818","D:\\output1"
		};

		// run job int status = new WCMapReduce().run(args);
		// System.exit(status);

		// run job
		int status = ToolRunner.run(configuration, new LogsCleanMR(), args);
		// exit program
		System.exit(status);
	}
}