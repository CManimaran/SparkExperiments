/**
 * 
 */
package com.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * @author Manimaran
 *
 */
public class SparkJsonParser {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkConf conf = new SparkConf().setAppName("SparkJsonParser").setMaster("local");
		SparkContext sc = new SparkContext(conf);
		
		
		System.out.println("JsonParser Start Time: " + sc.startTime());
		
		System.out.println("Executing...........: " + sc.deployMode());
		
		System.out.println("JsonParser End Time: " + sc.version());
		
	}

}
