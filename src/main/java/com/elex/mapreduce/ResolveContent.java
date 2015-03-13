package com.elex.mapreduce;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.elex.utils.DataClean;


public class ResolveContent {

	public static class MapTest extends Mapper<Object, Text, Text, IntWritable>{
		private Text userID = new Text();
		private IntWritable wordCount = new IntWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			StringTokenizer tokenIter = new StringTokenizer(line, "\n");
			while(tokenIter.hasMoreTokens()){
				String tmp = tokenIter.nextToken();
				StringTokenizer st = new StringTokenizer(tmp, "\t");
				String user = st.nextToken();
				st.nextToken();
				String content = st.nextToken();
				HashMap<String, Integer> wordMap = DataClean.clean(content);
				for(Map.Entry<String, Integer> wordEntry:wordMap.entrySet()) {
					String newKey = user + "\t" + wordEntry.getKey();
					userID.set(newKey);
					wordCount.set(wordEntry.getValue());
					context.write(userID, wordCount);
				}
			}
		} 
	}
	
	public static class ReduceTest extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable value:values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static class searchCountMap extends Mapper<Object, Text, Text, IntWritable>{
		private Text userID = new Text();
		private IntWritable wordCount = new IntWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String userTmp = value.toString();
			String[] userAndWord = userTmp.split("\t");
			String user = userAndWord[0];
			String sum = userAndWord[2];
			userID.set(user);
			wordCount.set(Integer.parseInt(sum));
			context.write(userID, wordCount);
		}
	}
	
	public static class searchCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable value:values){
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static class idfMap extends Mapper<Object, Text, Text, DoubleWritable>{
		private DoubleWritable one = new DoubleWritable(1.0);
		Double all = 0.0;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Text word = new Text();
			String tmp = value.toString();
			StringTokenizer userAndWord = new StringTokenizer(tmp,"\n");
			while(userAndWord.hasMoreTokens()){
				String userWordTmp = userAndWord.nextToken();
				String[] userWord = userWordTmp.split("\t");
				word.set(userWord[1]);
				all++;
				context.write(word, one);
			}
		}
		public void cleanup(Context context) throws IOException, InterruptedException{
			DoubleWritable total = new DoubleWritable(all);
			Text totalName = new Text();
			totalName.set("!!total");
			context.write(totalName, total);
		}
	}
	
	public static class idfReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		Double totalFile = 0.0;
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			DoubleWritable result = new DoubleWritable();
			//FloatWritable result = new FloatWritable();
			if(key.toString().indexOf("!!total") != -1){
				for(DoubleWritable value:values){
					totalFile += value.get();
				}
				result.set(totalFile);
				context.write(key, result);
				return;
			}
			Double sum = 0.0;
			for(DoubleWritable value:values){
				sum += value.get();
				
			}
			Double idf = totalFile/sum;
			result.set(idf);
			String keyStr = key.toString() + "\t" + sum + "****";
			Text keyT = new Text();
			keyT.set(keyStr);
			context.write(keyT, result);
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();	
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 4){
			 System.err.println("Usage:wordcountTest <in> <out>"); 
			 System.exit(2);
		}
		Job job = new Job(conf);
		job.setJobName("count");
		job.setJarByClass(ResolveContent.class);
		job.setMapperClass(MapTest.class);
		job.setCombinerClass(ReduceTest.class);
		job.setReducerClass(ReduceTest.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//System.exit(job.waitForCompletion(true)?0:1);
		
		Job searchCountJob = new Job(conf);
		searchCountJob.setJobName("searchcount");
		searchCountJob.setJarByClass(ResolveContent.class);
		searchCountJob.setMapperClass(searchCountMap.class);
		searchCountJob.setCombinerClass(searchCountReduce.class);
		searchCountJob.setReducerClass(searchCountReduce.class);
		searchCountJob.setOutputKeyClass(Text.class);
		searchCountJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(searchCountJob, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(searchCountJob, new Path(otherArgs[2]));
		//System.exit(searchCountJob.waitForCompletion(true)?0:1);
		
		Job idfJob = new Job(conf);
		idfJob.setJobName("idfJob");
		idfJob.setJarByClass(ResolveContent.class);
		idfJob.setMapperClass(idfMap.class);
		idfJob.setCombinerClass(idfReduce.class);
		idfJob.setReducerClass(idfReduce.class);
		idfJob.setOutputKeyClass(Text.class);
		idfJob.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(idfJob, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(idfJob, new Path(otherArgs[3]));
//		System.exit(idfJob.waitForCompletion(true)?0:1);
		
		ControlledJob controlledjob1 = new ControlledJob(job.getConfiguration());
		controlledjob1.setJob(job);
		
		ControlledJob idfControlledjob = new ControlledJob(idfJob.getConfiguration());
		idfControlledjob.setJob(idfJob);
		idfControlledjob.addDependingJob(controlledjob1);
		
		ControlledJob controlledjob2 = new ControlledJob(searchCountJob.getConfiguration());
		controlledjob2.setJob(searchCountJob);
		controlledjob2.addDependingJob(controlledjob1);
		
		JobControl jobControl = new JobControl("JobControl");
		jobControl.addJob(controlledjob1);
		jobControl.addJob(controlledjob2);
		jobControl.addJob(idfControlledjob);
		
		Thread thread = new Thread(jobControl);
		thread.start();
		while (true) {

            if (jobControl.allFinished()) {
            	// 如果作业成功完成，就打印成功作业的信息
                System.out.println(jobControl.getSuccessfulJobList());
                jobControl.stop();
                break;
            }
        }
		
	}

}
