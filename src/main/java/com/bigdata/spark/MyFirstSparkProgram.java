package com.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class MyFirstSparkProgram {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MyFirstSparkProgram").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);
    
        JavaRDD<Integer> rdd = context.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        
        rdd.foreach(x -> System.out.println(x));
    
        List<Integer> list = rdd.collect();
        for (Integer i: list) {
            System.out.println(i);
        }
        
        JavaRDD<Integer> squareRDD = rdd.map(x -> x*x);
        squareRDD.foreach(x -> System.out.println(x));
        
        JavaRDD<Integer> evenRDD = rdd.filter(x ->  x % 2 == 0);
        
        evenRDD.foreach(x -> System.out.println(x));
        
        JavaRDD<Integer> oddRDD = rdd.filter(
                new Function<Integer, Boolean>() {
                    @Override
                    public Boolean call(Integer integer) throws Exception {
                        return integer % 2 != 0;
                    }
                }
        );
        
        oddRDD.foreach(x -> System.out.println(x));
    }
}
