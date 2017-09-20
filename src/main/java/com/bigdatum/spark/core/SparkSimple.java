/*
 *  Implementing the Map, Filter and FlatMap from the Spark Core
 *  Map :		map(f : T ⇒ U)	: JavaRDD<T> n  ⇒  JavaRDD<U> n
 *  Filter :	filter(f : T ⇒ Bool) : JavaRDD<T> n ⇒  JavaRDD<T> m≤n
 *  FlatMap :	flatMap(f : T ⇒ List<U>) : JavaRDD<T> n ⇒ JavaRDD<U> m≥n
 */

package com.bigdatum.spark.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.*;

public class SparkSimple 
{
    public static void main( String[] args )
    {
    	Logger.getLogger("org").setLevel(Level.OFF);
    	Logger.getLogger("akka").setLevel(Level.OFF);
    	
    	SparkConf conf = new SparkConf();
    	conf.setMaster("local[2]").setAppName("First Program");
    	JavaSparkContext jsc = new JavaSparkContext(conf);
    	JavaRDD<Integer> numberRdd = jsc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
    	JavaRDD<Integer> squareRDD = numberRdd.map(x -> x*x);
    	JavaRDD<Integer> evenRDD = numberRdd.filter(x -> x%2 == 0);
    	JavaRDD<Integer> multipliedRDD = numberRdd.flatMap(x -> Arrays.asList(x, x*2,x*3).iterator());
    	List<Integer> numList = numberRdd.collect();
    	
    	
    	System.out.println("Number    RDD :- " + numberRdd.collect().toString());
    	System.out.println("Square    RDD :- " + squareRDD.collect().toString());
    	System.out.println("Even   	  RDD :- " + evenRDD.collect().toString());
    	System.out.println("Multipled RDD :- " + multipliedRDD.collect().toString());
    	System.out.println("Number List	:- " + numList);
    	System.out.println("Take 		:- " + numberRdd.take(5));
    	System.out.println("Count		:- " + numberRdd.count());
    	System.out.println("Reduce		:- " + numberRdd.reduce((n1,n2) -> n1+n2));
    	jsc.close();	
    }
}
