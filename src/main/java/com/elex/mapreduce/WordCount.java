package com.elex.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCount {
	
	public static class countMap extends Mapper<Text, Text, Text, LongWritable>{
		private static IntWritable one = new IntWritable(1);
		public void map(Text key, Text value, Context context){
			String tmp = value.toString();
			String[] words = tmp.split("\t");
			for(String word:words){
				
			}
		}
	}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		float i = 0.0f;
		System.out.println(i+1);
	}

}
