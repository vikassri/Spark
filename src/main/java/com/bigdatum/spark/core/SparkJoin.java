/* 
 * Implementing the Spark Join 
 * Join :	join()	: (JavaPairRDD<K,V> n1 , JavaPairRDD<K,W> n2) ⇒ JavaPairRDD<K,Tuple2<V,W>> m
 */

package com.bigdatum.spark.core;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkJoin {
	public static void main(String[] args){
		Logger.getLogger("org").setLevel(Level.OFF);
    	Logger.getLogger("akka").setLevel(Level.OFF);
    	
		SparkConf conf = new SparkConf();
		conf.setMaster("local[2]").setAppName("Spark Joins");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaPairRDD<String, String> visitsRDD = JavaPairRDD.fromJavaRDD(jsc.parallelize(
												Arrays.asList(
													new Tuple2<String,String>("index.html","10.20.30.10"),
													new Tuple2<String,String>("aboutme.html","10.20.30.40"),
													new Tuple2<String,String>("home.html","10.20.30.20"))));
		JavaPairRDD<String, String> mapRDD = JavaPairRDD.fromJavaRDD(jsc.parallelize(
												Arrays.asList(
													new Tuple2<String, String>("index.html","www.google.com"),
													new Tuple2<String, String>("aboutme.html","www.Yahoo.com"),
													new Tuple2<String, String>("test.html","www.facebook.com"))));
		JavaPairRDD<String,Tuple2<String,String>> joinRDD = visitsRDD.join(mapRDD);
		System.out.println("Visit RDD 	:" + visitsRDD.collect().toString());
		System.out.println("Map RDD 	:" + mapRDD.collect().toString());
		
// printing the common keys of both RDD's its similar to having Inner join 
		System.out.println("Join RDD 	:" + joinRDD.collect().toString());
		
		jsc.close();
	}

}
