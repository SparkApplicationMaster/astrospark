package crossmatch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.analysis.function.Sqrt;
import org.apache.hadoop.fs.Hdfs;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.netlib.util.doubleW;

import breeze.linalg.max;
import breeze.linalg.randomInt;
import breeze.linalg.scaleAdd;
import healpix.essentials.HealpixProc;
import healpix.essentials.Pointing;
import healpix.essentials.Vec3;
import scala.Tuple2;

public class Crossmatch {

	
	static int binSearch(ArrayList<Elem> a, Elem x, final double maxDist) {
		for (int i = 0, j = a.size(); i < j; ) {
			int m = (i + j) / 2;
			double dist = a.get(m).dummyFastDist(x);
			if (dist < -maxDist) {
				i = m + 1;
			} else if (dist > maxDist) {
				j = m;
			} else {
				return m;
			}
		}
		return -1;
	}
	
	static PairFlatMapFunction<Tuple2<Long, Tuple2<Iterable<Elem>,Iterable<Elem>>>, Long, Tuple2<Long,Double>> 
	nearestOneCell = new PairFlatMapFunction<Tuple2<Long, Tuple2<Iterable<Elem>, Iterable<Elem>>>, Long, Tuple2<Long, Double>>() {
		@Override
		public Iterator<Tuple2<Long, Tuple2<Long, Double>>> call(
				Tuple2<Long, Tuple2<Iterable<Elem>, Iterable<Elem>>> pixCat1Cat2) throws Exception {
					final double maxDist = 2 * Math.sin(Math.toRadians(1.0/3600.0) * 0.5);
					
					Iterable<Elem> iterCat1 = pixCat1Cat2._2()._1(), cat2 = pixCat1Cat2._2()._2();
					ArrayList<Tuple2<Long, Tuple2<Long, Double>>> result = new ArrayList<>();
					
					ArrayList<Elem> cat1 = new ArrayList<>();
					for (Elem e : iterCat1) {
						cat1.add(e);
					}
					
					Collections.sort(cat1, new Comparator<Elem>() {
						@Override
						public int compare(Elem e1, Elem e2) {
							return Double.compare(e1.dummyFastDist(e2), 0);
						}
					});
					
					Elem [] matches = new Elem[cat1.size()];
					double [] dists = new double[cat1.size()];
					Arrays.fill(dists, maxDist);
					
					for (Elem e2 : cat2) {
						int index = binSearch(cat1, e2, maxDist);
						if (index < 0) { continue; }
						
						double minDist = maxDist;
						int nearestIndex = index;
						
						for (int i = index; (i >= 0) && (e2.dummyFastDist(cat1.get(i)) < maxDist); --i) {
							double dist = e2.dist(cat1.get(i));
							if (dist < minDist) {
								minDist = dist;
								nearestIndex = i;
							}
						}
						
						for (int i = index + 1; (i < cat1.size()) &&  (cat1.get(i).dummyFastDist(e2) < maxDist); ++i) {
							double dist = e2.dist(cat1.get(i));
							if (dist < minDist) {
								minDist = dist;
								nearestIndex = i;
							}
						}
						
						if (minDist < dists[nearestIndex]) {
							dists[nearestIndex] = minDist;
							matches[nearestIndex] = e2;
						}
					}
					
					for (int i = 0; i < dists.length; ++i) {
						if (matches[i] == null) { continue; }
						result.add(new Tuple2<Long, Tuple2<Long, Double>>(cat1.get(i).id, new Tuple2<Long, Double>(matches[i].id, dists[i] / maxDist)));
					}
					
					return result.iterator();
				}
	};
	
	
	public static int run(String [] args, SparkConf sparkConf) {
		// TODO Auto-generated method stub
		if (args.length < 3) {
	      System.err.println("Usage: Crossmatch <file1> <file2> <output> [min_partitions] [healpix]");
	      return 1;
		}
		
		
		long tStart = System.currentTimeMillis();
		
		sparkConf.setAppName("Crossmatch");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.hadoopConfiguration().setLong("fs.local.block.size", 256 * 1024 * 1024);

	    boolean borderCheck = true;
	    boolean tryFilter = false;

	    int partitions = 16;
	    
	    if (args.length >= 3) {
	    	try {
	    		partitions = Integer.parseInt(args[3]);
	    	} catch (Exception e) {
				partitions = 16;
			} {
	    		
	    	}
	    }
	    
	    JavaRDD<String> xLines = sc.textFile(args[0]);
	    JavaRDD<String> yLines = sc.textFile(args[1]);

	    JavaPairRDD<Long,Elem> xElems = null;
	    JavaPairRDD<Long,Elem> yElems = null;
	    
	    int tryLowLevel;
	    final Broadcast<Integer> highLevel = sc.broadcast(18);
	    
	    if(tryFilter) {
	     	tryLowLevel = 11;
	    }
	    else {
	    	try {
	    		tryLowLevel = Integer.parseInt(args[4]);
	    	} catch (Exception e) {
	    		tryLowLevel = 5;
	    	}
	    	
	    }
	    final Broadcast<Integer> lowLevel = sc.broadcast(tryLowLevel);
	    
	    PairFlatMapFunction<String, Long, Elem> parseWithBorderCheck = new PairFlatMapFunction<String, Long, Elem>() {
    		
    		@Override
    		public Iterator<Tuple2<Long, Elem>> call(String arg0) throws Exception {
    			ArrayList<Tuple2<Long, Elem>> elems = new ArrayList<>();
    			try {
    				String [] parts = arg0.split(",");
    		        long id = Long.parseLong(parts[0]);
    		        
    		        Pointing p = new Pointing(Math.toRadians(90 - Double.parseDouble(parts[2])),
    		        		Math.toRadians(360 - Double.parseDouble(parts[1])));
    		        
    		        long littlePix = HealpixProc.ang2pixNest(highLevel.value(), p);
    		        
    		        long [] neighbours = HealpixProc.neighboursNest(highLevel.value(), littlePix);
    		        HashSet<Long> bigPixes = new HashSet<>();
    		        int difference = 2 * (highLevel.value() - lowLevel.value());
    		        bigPixes.add(littlePix >>> difference);
    		        for (long neighbour : neighbours) {
    					bigPixes.add(neighbour >>> difference);
    				}

    				for (Long pix : bigPixes) {
    					elems.add(new Tuple2<Long, Elem>(pix, new Elem(id, p)));
    				}
    				
    			} catch (Exception e) {
    				e.printStackTrace();
    			}
    			return elems.iterator();
    		}
    	};
    	
    	PairFlatMapFunction<String, Long, Elem> parse = new PairFlatMapFunction<String, Long, Elem>() {
    		
    		@Override
    		public Iterator<Tuple2<Long, Elem>> call(String arg0) throws Exception {
    			ArrayList<Tuple2<Long, Elem>> elems = new ArrayList<>();

    			try {
    				String [] parts = arg0.split(",");
    		        long id = Long.parseLong(parts[0]);
    		        
    		        Pointing p = new Pointing(Math.toRadians(90 - Double.parseDouble(parts[2])),
    		        		Math.toRadians(360 - Double.parseDouble(parts[1])));
    		       
    		        elems.add(new Tuple2<Long, Elem>(HealpixProc.ang2pixNest(lowLevel.value(), p), new Elem(id, p)));
    			} catch (Exception e) {
    				e.printStackTrace();
    			}
    			return elems.iterator();
    		}
    	};
	
	    if(tryFilter) 
	    {
	    	
	    	xElems = xLines.flatMapToPair(borderCheck ? parseWithBorderCheck : parse);
	    	
		    final Broadcast<HashSet<Long>> xKeys = sc.broadcast(new HashSet<Long>(xElems.keys().distinct().collect()));
		    
		    PairFlatMapFunction<String, Long, Elem> parseFilterByCell = new PairFlatMapFunction<String, Long, Elem>() {
	    		
	    		@Override
	    		public Iterator<Tuple2<Long, Elem>> call(String arg0) throws Exception {
	    			ArrayList<Tuple2<Long, Elem>> elems = new ArrayList<>();
	    			try {
	    				String [] parts = arg0.split(",");
	    		        long id = Long.parseLong(parts[0]);
	    		        
	    		        Pointing p = new Pointing(Math.toRadians(90 - Double.parseDouble(parts[2])),
	    		        		Math.toRadians(360 - Double.parseDouble(parts[1])));
	    		        
	    		        long pix = HealpixProc.ang2pixNest(lowLevel.value(), p);
	    		       
	    		        if (xKeys.value().contains(pix)) {
	    		        	elems.add(new Tuple2<Long, Elem>(pix, new Elem(id, p)));
	    		        }
	    			} catch (Exception e) {
	    				e.printStackTrace();
	    			}
	    			return elems.iterator();
	    		}
	    	};
			
			yElems = yLines.flatMapToPair(parseFilterByCell);
	    } 
	    else 
	    {
	    	xElems = xLines.flatMapToPair(borderCheck ? parseWithBorderCheck : parse);
	    	yElems = yLines.flatMapToPair(parse);
	    	partitions = Math.max(partitions, yElems.getNumPartitions());
	    }
		
		System.out.println("number of partitions: " + partitions);
		
		JavaPairRDD<Long, Iterable<Elem>> xGroup = xElems.groupByKey(partitions);
	    JavaPairRDD<Long, Iterable<Elem>> yGroup = yElems.groupByKey(partitions);

	    JavaPairRDD<Long, Tuple2<Iterable<Elem>, Iterable<Elem>>> z = xGroup.join(yGroup, partitions);

	    JavaPairRDD<Long, Tuple2<Long, Double>> w = z.flatMapToPair(nearestOneCell);
	    
	    if (borderCheck) {
	    	w = w.reduceByKey(new Function2<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>>() {
				@Override
				public Tuple2<Long, Double> call(Tuple2<Long, Double> match1, Tuple2<Long, Double> match2)
						throws Exception {
					return match1._2() <= match2._2() ? match1 : match2;
				}
			});
	    }
	 	    
	    try {
			FileUtils.deleteDirectory(new File(args[2]));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	    /*if (realXPart < numCores) {
	    	w = w.sortByKey().coalesce(1);
	    }*/
	    
	    w.saveAsTextFile(args[2]);
	    
	    long tEnd = System.currentTimeMillis();
	    long tDelta = tEnd - tStart;
	    double elapsedSeconds = tDelta / 1000.0;
	    
	    
	    System.out.println("crossmatch finished in " + elapsedSeconds);
		return 0;
	}
}
