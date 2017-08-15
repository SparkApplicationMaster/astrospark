package crossmatch;

import org.apache.spark.SparkConf;

public class Cluster {

	public static void main(String[] args) {
	    Crossmatch.run(args, new SparkConf());
	}

}
