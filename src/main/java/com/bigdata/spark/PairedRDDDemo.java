package com.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class PairedRDDDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PairedRDD").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);
    
        JavaRDD<String> rdd = context.parallelize(Arrays.asList("Pig", "Hadoop", "Java", "Pig", "Lambda", "Java", "Hadoop", "Pig", "Pig", "Hadoop"));
        JavaPairRDD<String, Integer>  pairRDD = rdd.mapToPair(x -> new Tuple2<String, Integer>(x, 1));
        
        pairRDD.foreach((x) -> System.out.println(x._1 + " " +x._2));
        
        JavaPairRDD<String, Integer> countRdd = pairRDD.reduceByKey((x ,y ) -> x + y);
        
        countRdd.foreach(x -> System.out.println(x._1 + " " + x._2));
        context.close();
    }
}
