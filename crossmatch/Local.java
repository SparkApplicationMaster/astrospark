package crossmatch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import java.util.HashSet;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;

import healpix.essentials.MocFitsIO;

public class Local {
	
	public static void main(String[] args) throws Exception {

		SparkConf sparkConf = new SparkConf();
		sparkConf
		.set("spark.master", "local[4]")
		.set("spark.driver.maxResultSize", "2g")
        .set("spark.executor.memoryOverhead", "200m")
        .set("spark.executor.memory", "3g")
        .set("spark.worker.memory", "12g")
        .set("spark.driver.memory", "2g")
		.set("spark.sql.warehouse.dir", "file:///c:/Spark/spark-warehouse")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
	    //Crossmatch.run(args, sparkConf);
	    //CrossmatchHash.run(args, sparkConf);
		

		args = Arrays.copyOf(args, args.length + 4);
		
		args[args.length - 4] = "" + 0;
		args[args.length - 3] = "" + 8;
		args[args.length - 2] = "" + 100000000;
		args[args.length - 1] = "C://Data/Crossmatch/las-Y-DR10.fits";
		
		/*int differ = 0;
		for (int i = 0; i < 100; ++i) {
			differ += CorrelateSort.run(args, sparkConf);
			System.out.println(differ);
		}*/
		
		Correlate.run(args, sparkConf);
	    
	    //try { System.in.read(); } catch (IOException e) { e.printStackTrace(); }
	    
	    System.exit(1);
	}

}
