package com.spark.java;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// Create SparkConf and JavaSparkContext
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		// Load text file into JavaRDD
		JavaRDD<String> lines = jsc.textFile("F:\\input\\datagen_10.txt");

		// Split lines into words
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) {
				// TODO Auto-generated method stub
				return (Iterator<String>) Arrays.asList(s.split(" "));
			}
		});

		JavaPairRDD<String, Integer> count = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String x) {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(x, 1);
			}

		});

		count.saveAsTextFile("F:\\output\\spark\\wordcount");
		
		jsc.stop();

	}
}
