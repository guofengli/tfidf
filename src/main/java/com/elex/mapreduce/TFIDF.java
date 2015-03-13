package com.elex.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.elex.utils.DataClean;

public class TFIDF {
	public static String hdfsURL = "hdfs://10.1.20.241:8020";
	public static String fileURL = "/tmp/userCount";
	public static class TFMap extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String userWordtmp = value.toString();
			StringTokenizer userWord = new StringTokenizer(userWordtmp, "\n");
			while (userWord.hasMoreTokens()) {
				String userAndWord = userWord.nextToken();
				StringTokenizer fragUserAndWord = new StringTokenizer(
						userAndWord, ",");
				String user = fragUserAndWord.nextToken();
				//fragUserAndWord.nextToken();
				if(fragUserAndWord.hasMoreTokens()){
					String content = fragUserAndWord.nextToken();
					ArrayList<String> wordList = DataClean.content2List(content);
					Text userID = new Text();
					userID.set(user);
					for (String word : wordList) {
						Text outputValue = new Text();
						outputValue.set(word);
						context.write(userID, outputValue);
					}
				}
			}
		}
	}

	public static class TFReduce extends Reducer<Text, Text, Text, Text> {
		public static long userCount = 0;
		public void setup(Context context) throws IOException{
			Configuration conf = context.getConfiguration();
			FileSystem fs = null;
			fs = FileSystem.get(URI.create(hdfsURL), conf);
			Path path = new Path(fileURL);
			if(!fs.isFile(path)){
				FSDataOutputStream output = fs.create(path, true);
				output.close();
			}
			FSDataInputStream input = fs.open(path);
			byte[] bytes = new byte[1024];
			StringBuffer sb = new StringBuffer();
			int status = input.read(bytes) ;
			while(status != -1){
				String str = new String(bytes);
				sb.append(str);
				status = input.read(bytes);
			}
			if(!"".equals(sb.toString())){
				userCount += Integer.parseInt(sb.toString().trim());
			}
			input.close();
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> wordMap = new HashMap<String, Integer>();
			int total = 0;
			for (Text value : values) {
				String word = value.toString();
				if (wordMap.containsKey(word)) {
					int valuetmp = wordMap.get(word);
					wordMap.put(word, valuetmp + 1);
				} else {
					wordMap.put(word, 1);
				}
				total++;
			}
			String keyStr = key.toString();
			for (Map.Entry<String, Integer> words : wordMap.entrySet()) {
				int wordCount = words.getValue();
				String outputKeyStr = words.getKey() + "\t" + keyStr;
				float tf = (float) wordCount / (float) total;
				Text outputKey = new Text();
				Text outputValue = new Text();
				outputKey.set(outputKeyStr);
				outputValue.set(Float.toString(tf));
				context.write(outputKey, outputValue);
			}
			userCount++;
		}
		
		public void cleanup(Context context) throws IOException{
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

	public static class IDFMap extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String userWordtmp = value.toString();
			StringTokenizer userWords = new StringTokenizer(userWordtmp, "\n");
			while(userWords.hasMoreTokens()){
				String userWord = userWords.nextToken();
				StringTokenizer fragUserWord = new StringTokenizer(userWord, "\t");
				String word = fragUserWord.nextToken();
				String user = fragUserWord.nextToken();
				String tf = fragUserWord.nextToken();
				String outputValueStr = user + "\t" + tf;
				Text outputKey = new Text();
				Text outputValue = new Text();
				outputKey.set(word);
				outputValue.set(outputValueStr);
				context.write(outputKey, outputValue);
			}
		}
	}

	
	public static class IDFReduce extends
			Reducer<Text, Text, Text, Text> {
		public static long userCount = 0;
		public void setup(Context context) throws IOException{
			Configuration conf = context.getConfiguration();
			FileSystem fs = null;
			fs = FileSystem.get(URI.create(hdfsURL), conf);
			Path path = new Path(fileURL);
			FSDataInputStream input = fs.open(path);
			byte[] bytes = new byte[1024];
			StringBuffer sb = new StringBuffer();
			int status = input.read(bytes) ;
			while(status != -1){
				String str = new String(bytes);
				sb.append(str);
				status = input.read(bytes);
			}
			if(!"".equals(sb)){
				userCount += Long.parseLong(sb.toString().trim());
			}
		}
		
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int wordCount = 0;
			Iterator ite1 = values.iterator();
			LinkedList<String> list = new LinkedList<String>();
			while(ite1.hasNext()){
				wordCount++;
				list.add(ite1.next().toString());
			}
			Iterator ite2 =  list.iterator();
			float idftmp = (float)userCount/(float)(wordCount + 1);
			float idf = (float) Math.log(idftmp);
			while(ite2.hasNext()){
				String userTFtmp = ite2.next().toString();
				StringTokenizer userTFs = new StringTokenizer(userTFtmp, "\t");
				String user = userTFs.nextToken();
				String tfStr = userTFs.nextToken();
				float tf = Float.parseFloat(tfStr);
				float tfidf = tf*idf;
				String tfidfStr = Float.toString(tfidf);
				String outputValueStr = user + "\t" +tfidfStr;
				Text outputValue = new Text();
				outputValue.set(outputValueStr);
				context.write(key, outputValue);
			}		
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		conf.set("userCount", "0");
		Job tfjob = new Job(conf, "tfjob");
		tfjob.setJarByClass(TFIDF.class);
		tfjob.setMapperClass(TFMap.class);
		// tfjob.setCombinerClass(TFReduce.class);
		tfjob.setReducerClass(TFReduce.class);
		tfjob.setOutputKeyClass(Text.class);
		tfjob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(tfjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(tfjob, new Path(args[1]));
		tfjob.waitForCompletion(true);

		 Job job = new Job(conf, "idfjob");
		 job.setJarByClass(TFIDF.class);
		 job.setMapperClass(IDFMap.class);
//		 job.setCombinerClass(IDFReduce.class);
		 job.setReducerClass(IDFReduce.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 FileInputFormat.addInputPath(job, new Path(args[1]));
		 FileOutputFormat.setOutputPath(job, new Path(args[2]));
		 System.exit(job.waitForCompletion(true)?0:1);
	}
}
