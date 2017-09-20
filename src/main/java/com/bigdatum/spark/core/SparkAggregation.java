/*
 *  Implementing the spark ReduceByKey and GroupByKey 
 *  ReduceByKey : reduceByKey(f : (V,V) ⇒ V)  : JavaPairRDD<K,V> n  ⇒  JavaPairRDD<K,V> m≤n
 *  GroupByKey :  groupByKey()	: JavaPairRDD<K,V> n  ⇒  JavaPairRDD< K, Iterable<V> > m≤n
 */

package com.bigdatum.spark.core;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class SparkAverage 
{
	public static void main(String[] args)
	{
		Logger.getLogger("org").setLevel(Level.OFF);
    	Logger.getLogger("akka").setLevel(Level.OFF);
    	
		SparkConf conf = new SparkConf();
		conf.setMaster("local[2]").setAppName("Spark Average Programe");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> line = jsc.parallelize(Arrays.asList("how are","you vikas","all set","how are","you naresh"));
		JavaRDD<String> words = line.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
		JavaPairRDD<String,Integer> word = words.mapToPair(s -> new Tuple2<String, Integer>(s,1));
		JavaPairRDD<String,Integer> wordCount = word.reduceByKey((x,y) -> x+y);
		JavaPairRDD<String,Iterable<Integer>> wordCnt = word.groupByKey();
		System.out.println("\nReduceByKey : " + wordCount.collect().toString());
		System.out.println("GroupByKey  : " + wordCnt.collect().toString());
		jsc.close();
	}
}
