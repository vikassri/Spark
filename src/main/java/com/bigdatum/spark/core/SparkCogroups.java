/*
 * CoGroup: CoGroup(): (JavaPairRDD<K,V> n1 ,JavaPairRDD<K,W> n2) ⇒ JavaPairRDD< K ,Tuple2<Iterable<V>,Iterable<W>> > m
 */

package com.bigdatum.spark.core;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkCogroups {
	public static void main(String[] args){
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("org").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark CoGroup Example");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaPairRDD<String, String> visitsRDD = JavaPairRDD.fromJavaRDD(jsc.parallelize(Arrays.asList(
												new Tuple2<String,String>("home.html","10.20.30.40"),
												new Tuple2<String,String>("aboutme.html","10.20.30.40"),
												new Tuple2<String,String>("contact.html","10.20.30.40"))));
		JavaPairRDD<String, String> pageNamesRDD = JavaPairRDD.fromJavaRDD(jsc.parallelize(Arrays.asList(
												new Tuple2<String,String>("home.html","Home"),
												new Tuple2<String,String>("aboutme.html","aboutme"),
												new Tuple2<String,String>("contact.html","contact"))));
		JavaPairRDD<String, Tuple2<Iterable<String>,Iterable<String>>> joinRDD = visitsRDD.cogroup(pageNamesRDD);
		System.out.println("Visit RDD	: " + visitsRDD.collect().toString());
		System.out.println("PagesName RDD 	: " + pageNamesRDD.collect().toString());
		
		// printing the All keys of both RDD's its similar to having full outer join 
		System.out.println("CoGroup RDD 	: " + joinRDD.collect().toString());
		jsc.close();
	}
}
