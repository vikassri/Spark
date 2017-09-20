/*
 * Union :	union()	: ( JavaRDD<T> n1 , JavaRDD<T> n2     ) ⇒ JavaRDD<T> n1+n2
 */
package com.bigdatum.spark.core;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkUnion {
	public static void main(String[] args){
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf().setAppName("Spark Union Example").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<Integer> number1RDD = jsc.parallelize(Arrays.asList(1,3,5)); 
		JavaRDD<Integer> number2RDD = jsc.parallelize(Arrays.asList(2,4,6)); 
		
		JavaRDD<Integer> unionRDD = number1RDD.union(number2RDD);
		System.out.println("Number RDD1 : " + number1RDD.collect().toString());
		System.out.println("Number RDD2 : " + number2RDD.collect().toString());
		System.out.println("Union RDD : " + unionRDD.collect().toString());
		jsc.close();
	}
}
