import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountTest {
	public static String hdfsURL = "hdfs://10.1.20.241:8020";
	public static String fileURL = "/tmp/usercountTest";

	public static class MapTest extends Mapper<Object, Text, Text, IntWritable> {
		IntWritable one = new IntWritable(1);
		Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenIter = new StringTokenizer(line, "\n");
			while (tokenIter.hasMoreTokens()) {
				String tmp = tokenIter.nextToken();
				StringTokenizer st = new StringTokenizer(tmp, "\t");
				String user = st.nextToken();
				word.set(user);
				context.write(word, one);
			}
		}
	}

	public static class ReduceTest extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		long userCount = 0;

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = null;
			fs = FileSystem.get(URI.create(hdfsURL), conf);
			Path path = new Path(fileURL);
			if (!fs.exists(path)) {
				FSDataOutputStream output = fs.create(path);
				output.close();
			} else {
				FSDataInputStream input = fs.open(path);
				byte[] bytes = new byte[1024];
				StringBuffer sb = new StringBuffer();
				int status = input.read(bytes);
				while (status != -1) {
					String str = new String(bytes);
					sb.append(str);
					status = input.read(bytes);
				}
				if (!"".equals(sb.toString())) {
					userCount = Long.parseLong(sb.toString().trim());
				}
			}
		}

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : value) {
				sum += i.get();
			}
			result.set(sum);
			context.write(key, result);
			userCount++;
		}

		public void cleanup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = null;
			fs = FileSystem.get(URI.create(hdfsURL), conf);
			Path path = new Path(fileURL);
			String content = Long.toString(userCount);
			FSDataOutputStream output = fs.append(path);
			output.write(content.getBytes());
			output.close();
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:wordcountTest <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "count");
		job.setJarByClass(WordCountTest.class);
		job.setMapperClass(MapTest.class);
//		job.setCombinerClass(ReduceTest.class);
		job.setReducerClass(ReduceTest.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
