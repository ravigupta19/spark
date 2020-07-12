package com.movie;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class TupleSorter implements Comparator<Tuple2<String, Double>> , Serializable {
    private static final long serialVersionUID = 1L;
    
    @Override
    public int compare(Tuple2<String, Double> t1, Tuple2<String, Double> t2) {
        if (t1._2 > t2._2) {
            return 1;
        }
        
        if (t1._2 < t2._2) {
            return -1;
        }
        
        return 0;
    }
}
