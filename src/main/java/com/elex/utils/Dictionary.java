package com.elex.utils;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class Dictionary {
	public static String hdfsURL = "hdfs://10.1.20.241:8020";
	public static Configuration conf = null;
	public static FileSystem fs = null;
	public static String fileURL = "/dictionary/";
	public static HashSet<String> dictionary = new HashSet<String>();
	static {
		try {
			init();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void init() throws IOException {
		conf = new Configuration();
		fs = FileSystem.get(URI.create(hdfsURL), conf);
		Path directoryPath = new Path(fileURL);
		FileStatus[] hdfsFileList = fs.listStatus(directoryPath);
		Path[] pathList = FileUtil.stat2Paths(hdfsFileList);
		for (Path filePath : pathList) {
			byte[] bytes = new byte[1024];
			FSDataInputStream input = fs.open(filePath);
			int status = input.read(bytes);
			while (status != -1) {
				String tmp = new String(bytes);
				String[] wordList = tmp.split("\n");
				for (String word : wordList) {
					if (!word.isEmpty()) {
						dictionary.add(word);
					}
				}
				status = input.read(bytes);
			}
		}
	}

//	public static void main(String[] args) {
//		 for(String str:dictionary){
//		 System.out.println(str);
//		 }
//		 System.out.println(dictionary.get(0));
//	}
}
