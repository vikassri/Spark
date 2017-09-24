package com.bigdatum.spark.core;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCount");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> file = jsc.textFile("/Users/vikas/Data/indeed/test.tsv");
		JavaRDD<String> words = file.map(x ->x.toLowerCase())
								.filter(x -> x.length()>0)
								.flatMap(x ->  Arrays.asList(SPACE.split(x)).iterator());
		JavaPairRDD<String,Integer> word = words.mapToPair(x -> new Tuple2<String, Integer>(x,1))
										.reduceByKey((n1,n2) -> n1+n2);
		System.out.println("Total Number of Word: " + word.count());
		jsc.close();
		long endTime = System.currentTimeMillis();
		System.out.println("Total Time Taken By Code: " + (endTime-startTime));
	}

}
