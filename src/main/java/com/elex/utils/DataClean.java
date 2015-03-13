package com.elex.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

public class DataClean {
	public static HashMap<String, Integer> clean(String content, String total) {
		String[] words = content.split("[ :/\\?<>.'\"(),.!*-+=%#&\t]+");
		HashSet<String> dictionary = Dictionary.dictionary;
		HashMap<String, Integer> wordMap = new HashMap<String, Integer>();
		wordMap.put(total, 0);
		for (String word : words) {
			if (!"".equals(word)) {
				if (!dictionary.contains(word)) {
					if (wordMap.containsKey(word)) {
						int value = wordMap.get(word);
						wordMap.put(word, value + 1);
					} else {
						wordMap.put(word, 1);
					}
					int value = wordMap.get(total);
					wordMap.put(total, value + 1);
				}
			}
		}
		return wordMap;
	}

	public static HashMap<String, Integer> clean(String content) {
		String[] words = content.split("[ :/\\?<>.'\"(),.!*-+=%#&\t]+");
		HashSet<String> dictionary = Dictionary.dictionary;
		HashMap<String, Integer> wordMap = new HashMap<String, Integer>();
		for (String word : words) {
			if (!"".equals(word)) {
				if (!dictionary.contains(word)) {
					if (wordMap.containsKey(word)) {
						int value = wordMap.get(word);
						wordMap.put(word, value + 1);
					} else {
						wordMap.put(word, 1);
					}
				}
			}
		}
		return wordMap;
	}
	
	
	public static ArrayList<String> content2List(String content) {
		String[] words = content.split("[ :/\\?<>.'\"(),.!*-+=%#&\t]+");
		HashSet<String> dictionary = Dictionary.dictionary;
		ArrayList<String> wordList = new ArrayList<String>();
		for (String word : words) {
			if (!"".equals(word)) {
				if (!dictionary.contains(word)) {
					wordList.add(word);
				}
			}
		}
		return wordList;
	}

	public static HashMap<String, Integer> list2map(Iterable<Text> values, String totalStr) {
		HashMap<String, Integer> wordMap = new HashMap<String, Integer>();
		wordMap.put(totalStr, 0);
		for (Text value : values) {
			String word = value.toString();
			if (wordMap.containsKey(word)) {
				int valuetmp = wordMap.get(word);
				wordMap.put(word, valuetmp + 1);
			} else {
				wordMap.put(word, 1);
			}
			int valuetmp = wordMap.get(totalStr);
			wordMap.put(totalStr, valuetmp + 1);
		}
		return wordMap;
	}

	public static void main(String[] args) {
		// System.out.println(clean("http://www?bai du   com.<ni:>k/i.wo   cn org "));
		// String str = Dictionary.dictionary.get(0);
		// String[] list = str.split("\n");
//		HashMap<String, Integer> wordMap = clean("http://www?bai du   com.<ni:>k/i.wo   cn org [ai\tkkk", "!total");
//		System.out.println(wordMap.size());
//		for (Map.Entry<String, Integer> m : clean(
//				"apc  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct bills  apct bills  apct waybills  apct waybills  bible telugu version  bible telugu version  cafe seans  chevpromx live  chevpromx live  dhanalaxmi traders yahoo  facebook  facebook  girls boobs  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail log  gmail sign  gmail sign  google  google  google  google  google  google  gopala gopala telugu movie online  gopala gopala telugu movie online  internet cafe hot spot videos  kirai kotigadu movie online  kirai kotigadu movie online  narayana comedy  narayana comedy  recording dance dress  rprakash gmail  sex seans  sexy vedeos  sexy vedeos  sexy vedeos  sivananda gmail  temper  temper release states  udandapalem santthabumalimandalam  yahoo  yahoo  yahoo  yahoo  yahoo  yahoo  yahoo  yahoo  yahoo  yahoo  yahoo login  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail ").entrySet()) {
//			System.out.println(m.getKey() + "\t" + m.getValue().toString());
//		}
		
//		for(String word:content2List("http://www?bai du   com.<ni:>k/i.wo   cn org [ai\tkkk")){
//			System.out.println(word);
//		}
		
//		ArrayList<String> list = new ArrayList<String>();
//		list.add("32");
//		list.add("rre");
//		Iterator listIt = list.iterator();
//		Iterator it1 = listIt;
//		Iterator it2 = listIt;
//		while(it1.hasNext()){
//			System.out.println(it1.next());
//		}
//		while(it2.hasNext()){
//			System.out.println(it2.next());
//		}
//		
//		StringTokenizer str = new StringTokenizer("apc  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct  apct bills  apct bills  apct waybills  apct waybills  bible telugu version  bible telugu version  cafe seans  chevpromx live  chevpromx live  dhanalaxmi traders yahoo  facebook  facebook  girls boobs  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail  gmail log  gmail sign  gmail sign  google  google  google  google  google  google  gopala gopala telugu movie online  gopala gopala telugu movie online  internet cafe hot spot videos  kirai kotigadu movie online  kirai kotigadu movie online  narayana comedy  narayana comedy  recording dance dress  rprakash gmail  sex seans  sexy vedeos  sexy vedeos  sexy vedeos  sivananda gmail  temper  temper release states  udandapalem santthabumalimandalam  yahoo  yahoo  yahoo  yahoo  yahoo  yahoo  yahoo  yahoo  yahoo  yahoo  yahoo login  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail  yahoo mail ");
//		while(str.hasMoreTokens()){
//			System.out.println(str.nextToken());
//		}
		
//		StringBuffer st = new StringBuffer();
////		st.append("123654");
//		if(!"".equals(st.toString()))
//		System.out.println(Long.parseLong(st.toString().trim()));
//		System.out.println(Math.log((float)3343301/(float)2) * 0.010869565) ;
		
//		System.out.println(Dictionary.dictionary.size());
	}
}
