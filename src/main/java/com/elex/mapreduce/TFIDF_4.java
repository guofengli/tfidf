package com.elex.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.io.Closeables;

public class TFIDF_4 {
	public static String hdfsURL = "hdfs://10.1.20.241:8020";
	public static String fileURL = "/tmp/usercount";

//	public static class UserCountMap extends Mapper<Object, Text, Text, Text> {
//
//		public void map(Object key, Text value, Context context)
//				throws IOException, InterruptedException {
//			String userWordtmp = value.toString();
//			StringTokenizer userWord = new StringTokenizer(userWordtmp, "\n");
//			while (userWord.hasMoreTokens()) {
//				Text outputKey = new Text();
//				outputKey.set("usercount");
//				Text one = new Text();
//				one.set("1");
//				context.write(outputKey, one);
//			}
//		}
//	}
//
//	public static class UserCountReduce extends Reducer<Text, Text, Text, Text> {
//		public void reduce(Text key, Iterable<Text> values, Context context)
//				throws IOException, InterruptedException {
//			userCount = 0;
//			for (Text value : values) {
//				String valueTmp = value.toString();
//				userCount += Long.parseLong(valueTmp);
//			}
//		}
//	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
//		Configuration conf = new Configuration();
//		Job userCountJob = Job.getInstance(conf, "usercountjob");
//		userCountJob.setJarByClass(TFIDF_4.class);
////		userCountJob.setCombinerClass(UserCountCombine.class);
//		userCountJob.setReducerClass(UserCountReduce.class);
//		userCountJob.setOutputKeyClass(Text.class);
//		userCountJob.setOutputValueClass(Text.class);
//		FileInputFormat.setInputPaths(userCountJob, new Path(args[1]));
//		FileOutputFormat.setOutputPath(userCountJob, new Path(args[2]));
//		userCountJob.waitForCompletion(true);
		
		Configuration conf = new Configuration();
//		conf.set("mapred.child.java.opts", "-Xmx4096m");
		Job idfJob = Job.getInstance(conf, "idfjob");
		idfJob.setJarByClass(TFIDF_4.class);
		idfJob.setMapperClass(IDFMap.class);
		idfJob.setReducerClass(IDFReduce.class);
		idfJob.setOutputKeyClass(Text.class);
		idfJob.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(idfJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(idfJob, new Path(args[1]));
		idfJob.waitForCompletion(true);
		
		
		Counter  ct = idfJob.getCounters().findCounter("HAS_QUERY","USER_COUNT");
//		IOUtils.write(new Long(ct.getValue()).intValue(), System.out);
		  FileSystem fs = FileSystem.get(URI.create(hdfsURL), conf);
		  Path path = new Path("/tmp/tusercount");
		    FSDataOutputStream out = fs.create(path);
		    try {
		      out.writeInt(new Long(ct.getValue()).intValue());
		    } finally {
		      Closeables.closeQuietly(out);
		    }
	}

	public static class IDFMap extends Mapper<Object, Text, Text, Text>{
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
						outputValue.set(user + " " + tf);
						context.write(outputKey, outputValue);
					}
				}
			}
			
		}
	}
	
	public static class IDFReduce extends Reducer<Text, Text, Text, Text>{
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
			StringBuffer outputStr = new StringBuffer();
			while (userIter.hasNext()) {
				String usertftmp = userIter.next();
				StringTokenizer usertf = new StringTokenizer(usertftmp, " ");
				String user = usertf.nextToken();
				String tfStr = usertf.nextToken();
				float tf = Float.parseFloat(tfStr.trim().toString());
				float tfidf = tf * idf;
				String outputTmp = user + " " + tfidf;
//				outputStr.append(user);
//				outputStr.append(" ");
//				outputStr.append(tfidf);
//				outputStr.append(",");
				outputValue.set(outputTmp);
				context.write(key, outputValue);
			}
//			outputValue.set(outputStr.toString());
//			context.write(key, outputValue);
		}
	}
	
}
