package crossmatch;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.util.Saveable;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.module.scala.util.Strings;

import breeze.linalg.diff;
import scala.Tuple2;
import scala.Tuple3;
import scala.tools.scalap.scalax.util.StringUtil;
import scala.util.matching.Regex;

public class ResultCheck {

	public static void main(String[] args) {
		
		// TODO Auto-generated method stub
		if (args.length < 2) {
	      System.err.println("Usage: Crossmatch <file1> <file2>");
	      System.exit(1);
		}

		SparkConf sparkConf = new SparkConf();
		sparkConf
		.set("spark.master", "local[8]")
		.set("spark.driver.maxResultSize", "4g")
		.set("spark.shuffle.fraction", "0.4")
        .set("spark.executor.memoryOverhead", "600m")
        .set("spark.executor.memory", "3g")
        .set("spark.worker.memory", "24g")
        .set("spark.driver.memory", "4g")
		.set("spark.sql.warehouse.dir", "file:///c:/Spark/spark-warehouse");
		
	    SparkSession spark = SparkSession
	      .builder().config(sparkConf)
	      .appName("Crossmatch")
	      .getOrCreate();
	    
	    
	    /*JavaPairRDD<Long, Iterable<Tuple2<Long, Double>>> topcat = spark.read().textFile(args[1]).javaRDD().
	    		flatMapToPair((String s) -> {
	    			ArrayList<Tuple2<Long, Tuple2<Long, Double>>> elem = new ArrayList<>();
	    			try {
						String [] parts = s.split(",");
						elem.add(new Tuple2<Long, Tuple2<Long, Double>>(Long.parseUnsignedLong(parts[0]), 
								new Tuple2<Long, Double>(Long.parseUnsignedLong(parts[3]), Double.parseDouble(parts[6]))));
					} catch (Exception e) {
					}
	    			return elem.iterator();
	    		}).groupByKey(8);
	    System.out.println(topcat.count());
	    
	    
	    JavaPairRDD<Long, Iterable<Tuple2<Long, Double>>> my = spark.read().textFile(args[0]).javaRDD().
	    		flatMapToPair((String s) -> {
	    			ArrayList<Tuple2<Long, Tuple2<Long, Double>>> elem = new ArrayList<>();
	    			try {
						String [] parts = s.replace(")", "").replace("(", "").split(",");
						elem.add(new Tuple2<Long, Tuple2<Long, Double>>(Long.parseUnsignedLong(parts[0]), 
								new Tuple2<Long, Double>(Long.parseUnsignedLong(parts[1]), Double.parseDouble(parts[2]))));
					} catch (Exception e) {
						System.out.println(e.getMessage());
					}
	    			return elem.iterator();
	    		}).groupByKey(8);
	    
	    System.out.println(my.count());
	    
	    JavaPairRDD<Long, Tuple2<Iterable<Tuple2<Long, Double>>, Iterable<Tuple2<Long, Double>>>> merge = topcat.join(my, 8);
	    System.out.println(merge.count());
	    
	    //merge.saveAsTextFile("C://Data/Crossmatch/ResultCheck");
	    
	    System.out.println(merge.mapValues((Tuple2<Iterable<Tuple2<Long, Double>>, Iterable<Tuple2<Long, Double>>> match) -> match._1().iterator().next()._1() - match._2().iterator().next()._1())
	    		.reduce((a, b) -> new Tuple2<Long, Long>(0L, a._2() + b._2()))._2());
	    */
	}

}
