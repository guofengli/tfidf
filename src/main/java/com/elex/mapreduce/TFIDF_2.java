package com.elex.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.elex.utils.DataClean;

public class TFIDF_2 {
	public static String hdfsURL = "hdfs://10.1.20.241:8020";
	public static String fileURL = "/tmp/usercount";
	public static long userCount = 0;
	public static class TFMap extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String userWordstmp = value.toString();
			StringTokenizer userWords = new StringTokenizer(userWordstmp, "\n");
			while (userWords.hasMoreTokens()) {
				String userWordFragtmp = userWords.nextToken();
				StringTokenizer userWordFrag = new StringTokenizer(
						userWordFragtmp, ",");
				String user = userWordFrag.nextToken();
				while (userWordFrag.hasMoreTokens()) {
					String words = userWordFrag.nextToken();
					HashMap<String, Integer> wordMap = DataClean.clean(words,
							"!total");
					int wordTotal = wordMap.get("!total");
					wordMap.remove("!total");
					for (Map.Entry<String, Integer> wordEntry : wordMap
							.entrySet()) {
						Text outputKey = new Text();
						Text outputValue = new Text();
						String word = wordEntry.getKey();
						int wordCount = wordEntry.getValue();
						float tf = (float) wordCount / (float) wordTotal;
						outputKey.set(user);
						outputValue.set(word + "\t" + Float.toString(tf));
						context.write(outputKey, outputValue);
					}
				}
			}
		}
	}

	public static class TFReduce extends Reducer<Text, Text, Text, Text> {
//		public long userCount = 0;
//		public Configuration conf = null;
//		public Path path = null;
//		public FileSystem fs = null;
//
//		public void setup(Context context) throws IOException {
//			conf = context.getConfiguration();
//			path = new Path(fileURL);
//			fs = FileSystem.get(URI.create(hdfsURL), conf);
//			if (!fs.isFile(path)) {
//				FSDataOutputStream output = fs.create(path, true);
//				output.close();
//			}
//			FSDataInputStream input = fs.open(path);
//			StringBuffer sb = new StringBuffer();
//			byte[] bytes = new byte[1024];
//			int status = input.read(bytes);
//			while (status != -1) {
//				sb.append(new String(bytes));
//				
//				status = input.read(bytes);
//			}
//			if (!"".equals(sb.toString())) {
//				userCount = Long.parseLong(sb.toString().trim());
//			}
//			input.close();
//		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			Iterator<Text> iter = values.iterator();
			while (iter.hasNext()) {
				sb.append(iter.next().toString() + "\t");
			}
			Text outputValue = new Text();
			outputValue.set(sb.toString());
			context.write(key, outputValue);
//			userCount++;
		}

//		public void cleanup(Context context) throws IOException {
//			FSDataOutputStream output = fs.create(path, true);
//			String content = Long.toString(userCount);
//			output.write(content.getBytes());
//			output.flush();
//			output.close();
//		}
	}

	public static class TFCombine extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			Iterator<Text> iter = values.iterator();
			while (iter.hasNext()) {
				sb.append(iter.next().toString() + "\t");
			}
			Text outputValue = new Text();
			outputValue.set(sb.toString());
			context.write(key, outputValue);
		}
	}

	public static class IDFMap extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String userWordstmp = value.toString();
			StringTokenizer userWords = new StringTokenizer(userWordstmp, "\n");
			while (userWords.hasMoreTokens()) {
				String userWordsFragtmp = userWords.nextToken();
				StringTokenizer userWordsFrag = new StringTokenizer(
						userWordsFragtmp, "\t");
				String user = userWordsFrag.nextToken();
				while (userWordsFrag.hasMoreTokens()) {
					String word = userWordsFrag.nextToken();
					String tf = userWordsFrag.nextToken();
					Text outputKey = new Text();
					Text outputValue = new Text();
					outputKey.set(word);
					outputValue.set(user + "\t" + tf);
					context.write(outputKey, outputValue);
				}
			}
		}
	}

	public static class IDFReduce extends Reducer<Text, Text, Text, Text> {
//		public static long userCount;
//		public void setup(Context context) throws IOException {
//			Configuration conf = context.getConfiguration();
//			FileSystem fs = FileSystem.get(URI.create(hdfsURL), conf);
//			Path path = new Path(fileURL);
//			FSDataInputStream input = fs.open(path);
//			byte[] bytes = new byte[1024];
//			StringBuffer sb = new StringBuffer();
//			int status = input.read(bytes) ;
//			while(status != -1){
//				String str = new String(bytes);
//				sb.append(str);
//				status = input.read(bytes);
//			}
//			if(!"".equals(sb.toString())){
//				userCount = Long.parseLong(sb.toString().trim());
//			} else{
//				userCount = 0;
//			}
//			input.close();
//		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			LinkedList<String> userList = new LinkedList<String>();
			Iterator<Text> iter = values.iterator();
			long wordCount = 0;
			while (iter.hasNext()) {
				wordCount++;
				userList.add(iter.next().toString());
			}
			float idf = (float) Math.log((float) userCount
					/ (float) (wordCount + 1));
			Iterator<String> userIter = userList.iterator();
			while (userIter.hasNext()) {
				String usertftmp = userIter.next();
				StringTokenizer usertf = new StringTokenizer(usertftmp, "\t");
				String user = usertf.nextToken();
				String tfStr = usertf.nextToken();
				float tf = Float.parseFloat(tfStr.trim().toString());
				float tfidf = tf * idf;
				Text outputKey = new Text();
				Text outputValue = new Text();
				outputKey.set(user);
				outputValue.set(key.toString() + "\t" + tfStr + "\t"
						+ Float.toString(tfidf));
				context.write(outputKey, outputValue);
			}
		}
	}
	
	public static class UserCountMap extends Mapper<Object, Text, Text, LongWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String userWordtmp = value.toString();
			StringTokenizer userWord = new StringTokenizer(userWordtmp, "\n");
			while(userWord.hasMoreTokens()) {
				Text outputKey = new Text();
				outputKey.set("usercount");
				LongWritable one = new LongWritable(1);
				context.write(outputKey, one);
			}
		}
	}
	
	public static class UserCountCombine extends Reducer<Text, LongWritable, Text, LongWritable>{
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			long user = 0;
			for(LongWritable value:values){
				user += value.get();
			}
			LongWritable outputValue = new LongWritable(user);
			context.write(key, outputValue);
		}
	}
	
	public static class UserCountReduce extends Reducer<Text, LongWritable, NullWritable, NullWritable>{
		public void reduce(Text key, Iterable<LongWritable> values, Context context){
			userCount = 0;
			for(LongWritable value: values){
				userCount += value.get();
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("mapred.child.java.opts", "-Xmx2048m");
		 Job tfJob = Job.getInstance(conf,"tfjob");
		 tfJob.setJarByClass(TFIDF_2.class);
		 tfJob.setMapperClass(TFMap.class);
//		 tfJob.setCombinerClass(TFCombine.class);
		 tfJob.setReducerClass(TFReduce.class);
		 tfJob.setOutputKeyClass(Text.class);
		 tfJob.setOutputValueClass(Text.class);
		 FileInputFormat.setInputPaths(tfJob, new Path(args[0]));
		 FileOutputFormat.setOutputPath(tfJob, new Path(args[1]));
		 tfJob.waitForCompletion(true);

		 
		 Job userCountJob = Job.getInstance(conf,"usercountjob");
		 userCountJob.setJarByClass(UserCountMap.class);
		 userCountJob.setCombinerClass(UserCountCombine.class);
		 userCountJob.setReducerClass(UserCountReduce.class);
		 FileInputFormat.setInputPaths(userCountJob, new Path(args[1]));
		 userCountJob.waitForCompletion(true);
		 
		Job idfJob = Job.getInstance(conf, "idfjob");
		idfJob.setJarByClass(TFIDF_2.class);
		idfJob.setMapperClass(IDFMap.class);
		idfJob.setReducerClass(IDFReduce.class);
		idfJob.setOutputKeyClass(Text.class);
		idfJob.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(idfJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(idfJob, new Path(args[2]));
		System.exit(idfJob.waitForCompletion(true) ? 0 : 1);
	}

}
