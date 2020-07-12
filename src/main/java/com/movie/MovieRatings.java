package com.movie;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class MovieRatings {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Movie Ratings").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);
    
        JavaRDD<String> inputData = context.textFile("data/u.data");
    
        JavaPairRDD<String, Tuple2<Double, Integer>> movieRatings = inputData.mapToPair(x -> {
                String[] values = x.split("\t");
                return new Tuple2<String, Tuple2<Double, Integer>>(values[1], new Tuple2<Double, Integer>(Double.parseDouble(values[2]), 1));
            }
        );
        
        JavaPairRDD<String, Tuple2<Double, Integer>> movieRatingCount = movieRatings.reduceByKey((tuple1, tuple2) -> {
                return  new Tuple2<Double, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2);
            }
        );
        
        JavaPairRDD<String, Double> moviesAverageRatings = movieRatingCount.mapToPair(x -> {
            Double average = x._2._1 / x._2._2;
            return new Tuple2<String, Double>(x._1, average);
        });
        
        moviesAverageRatings.foreach(x -> System.out.println("Movie Id: " +  x._1 + " Average Ratings " + x._2));
        
        JavaRDD<String> inputItems = context.textFile("data/u.item");
        
        JavaPairRDD<String, String> infoLeanRDD = inputItems.mapToPair(x -> {
            String[] values = x.split(";");
            return new Tuple2<String, String>(values[0], values[1]);
        });
        
        JavaPairRDD<String, Tuple2<String, Double>> joinRDD = infoLeanRDD.join(moviesAverageRatings);
    
        JavaPairRDD<String, Double> leanInfoRDD = joinRDD.mapToPair(x -> new Tuple2<String, Double>(x._2._1, x._2._2));
        
        List<Tuple2<String, Double>> top20Movies = leanInfoRDD.top(20, new TupleSorter());
        
        top20Movies.forEach(x -> System.out.println(x._1 + " " + x._2));
        
    }
}
