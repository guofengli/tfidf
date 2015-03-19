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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.elex.mapreduce.TFIDF_4.IDFMap;
import com.elex.mapreduce.TFIDF_4.IDFReduce;
import com.elex.utils.DataClean;

public class TFIDF_5 {
	public static String hdfsURL = "hdfs://10.1.20.241:8020";
	public static String fileURL = "/tmp/usercount";
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
				Text outputKey = new Text();
				Text outputValue = new Text();
				while (userWordFrag.hasMoreTokens()) {
					String words = userWordFrag.nextToken();
					HashMap<String, Integer> wordMap = DataClean.clean(words,
							"!total");
					int wordTotal = wordMap.get("!total");
					wordMap.remove("!total");
					for (Map.Entry<String, Integer> wordEntry : wordMap
							.entrySet()) {
						String word = wordEntry.getKey();
						int wordCount = wordEntry.getValue();
						float tf = (float) wordCount / (float) wordTotal;
						String outputStr = word + " " + Float.toString(tf) + ",";
						byte []bytes = outputStr.getBytes();
						outputValue.append(bytes, 0, bytes.length);
					}
				}
				outputKey.set(user);
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class TFReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
//			StringBuffer sb = new StringBuffer();
			Iterator<Text> iter = values.iterator();
			while (iter.hasNext()) {
//				sb.append(iter.next().toString() + "\t");
				context.write(key, iter.next());
			}
//			Text outputValue = new Text();
//			outputValue.set(sb.toString());
//			context.write(key, outputValue);
		}
	}

	public static class IDFMap extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String valuesTmp = value.toString();
			StringTokenizer userWordFrag = new StringTokenizer(valuesTmp, "\n");
			while(userWordFrag.hasMoreTokens()){
				//String userWordtmp = userWordFrag.nextToken();
				StringTokenizer userWords = new StringTokenizer(userWordFrag.nextToken(), "\t");
				String user = userWords.nextToken();
				while(userWords.hasMoreTokens()){
					StringTokenizer wordTFs = new StringTokenizer(userWords.nextToken(), ",");
					while(wordTFs.hasMoreTokens()){
						StringTokenizer wordTF = new StringTokenizer(wordTFs.nextToken());
						String word = wordTF.nextToken();
						String tf = wordTF.nextToken();
						Text outputKey = new Text();
						Text outputValue = new Text();
						outputKey.set(word);
						outputValue.set(user + "\t" + tf);
						context.write(outputKey, outputValue);
					}
				}
			}
			
		}
	}

	public static class IDFReduce extends Reducer<Text, Text, Text, Text> {
		long userCount = 0;
		public void setup(Context context) throws IOException{
			Configuration conf = context.getConfiguration();
			Path path = new Path(fileURL);
			FileSystem fs = FileSystem.get(URI.create(hdfsURL), conf);
			if (!fs.isFile(path)) {
				FSDataOutputStream output = fs.create(path, true);
				output.close();
			}
			FSDataInputStream input = fs.open(path);
			StringBuffer sb = new StringBuffer();
			byte[] bytes = new byte[1024];
			int status = input.read(bytes);
			while (status != -1) {
				sb.append(new String(bytes));
				status = input.read(bytes);
			}
			if (!"".equals(sb.toString())) {
				userCount = Long.parseLong(sb.toString().trim());
			}
			input.close();
		}
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
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
			Text outputValue = new Text();
			while (userIter.hasNext()) {
				String usertftmp = userIter.next();
				StringTokenizer usertf = new StringTokenizer(usertftmp, "\t");
				String user = usertf.nextToken();
				String tfStr = usertf.nextToken();
				float tf = Float.parseFloat(tfStr.trim().toString());
				float tfidf = tf * idf;
				String outputTmp = user + "\t" + tfidf;
				outputValue.set(outputTmp);
				context.write(key, outputValue);
			}
		}	
	}

	public static class UserCountMap extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String userWordtmp = value.toString();
			StringTokenizer userWord = new StringTokenizer(userWordtmp, "\n");
			while (userWord.hasMoreTokens()) {
				userWord.nextToken();
				Text outputKey = new Text();
				outputKey.set("usercount");
				Text one = new Text();
				one.set("1");
				context.write(outputKey, one);
			}
		}
	}

	public static class UserCountCombine extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			long user = 0;
			for (Text value : values) {
				String valueTmp = value.toString();
				user += Long.parseLong(valueTmp);
			}
			Text outputValue = new Text();
			outputValue.set(Long.toString(user));
			context.write(key, outputValue);
		}
	}

	public static class UserCountReduce extends Reducer<Text, Text, Text, Text> {
		int userCount = 0;
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				String valueTmp = value.toString();
				userCount += Long.parseLong(valueTmp);
			}
		}
		public void cleanup(Context context) throws IOException{
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(URI.create(hdfsURL),conf);
			Path path = new Path(fileURL);
			FSDataOutputStream output = fs.create(path, true);
			String content = Long.toString(userCount);
			output.write(content.getBytes());
			output.flush();
			output.close();
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
//		conf.set("mapred.child.java.opts", "-Xmx4096m");
		Job tfJob = Job.getInstance(conf, "tfjob");
		tfJob.setJarByClass(TFIDF_5.class);
		tfJob.setMapperClass(TFMap.class);
		// tfJob.setCombinerClass(TFCombine.class);
		tfJob.setReducerClass(TFReduce.class);
		tfJob.setOutputKeyClass(Text.class);
		tfJob.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(tfJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(tfJob, new Path(args[1]));
		tfJob.waitForCompletion(true);
		
		Job userCountJob = Job.getInstance(conf, "usercountjob");
		userCountJob.setJarByClass(TFIDF_5.class);
		userCountJob.setMapperClass(UserCountMap.class);
		userCountJob.setCombinerClass(UserCountCombine.class);
		userCountJob.setReducerClass(UserCountReduce.class);
		userCountJob.setOutputKeyClass(Text.class);
		userCountJob.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(userCountJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(userCountJob, new Path(args[2]));
		userCountJob.waitForCompletion(true);

		Job idfJob = Job.getInstance(conf, "idfjob");
		idfJob.setJarByClass(TFIDF_5.class);
		idfJob.setMapperClass(IDFMap.class);
		idfJob.setReducerClass(IDFReduce.class);
		idfJob.setOutputKeyClass(Text.class);
		idfJob.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(idfJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(idfJob, new Path(args[3]));
		System.exit(idfJob.waitForCompletion(true) ? 0 : 1);
		
		
	}

}
