package crossmatch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Vector;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.netlib.util.doubleW;

import com.google.common.collect.HashMultimap;

import healpix.essentials.HealpixProc;
import healpix.essentials.Pointing;
import healpix.essentials.Vec3;
import scala.Tuple2;
import spire.random.DistSemiring;

public class CrossmatchHash {
	
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

	    
	    //xLines./*sample(false, 0.5).*/coalesce(1).saveAsTextFile("C://Data/Crossmatch/galex_q_1");
	    //yLines./*sample(false, 0.5).*/coalesce(1).saveAsTextFile("C://Data/Crossmatch/sdss_q_1");
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
	    
	    //xLines = xLines.coalesce(Math.max(1, (int)(xLines.getNumPartitions() / 16)));
	    //yLines = yLines.coalesce(Math.max(1, (int)(yLines.getNumPartitions() / 16)));

	    JavaPairRDD<Long,Elem> xElems = null;
	    JavaPairRDD<Long,Elem> yElems = null;
	    
	    int tryLowLevel;
	    final Broadcast<Integer> highLevel = sc.broadcast(10);
	    
	    if(tryFilter /*&& xPart < 8*/) {
	     	tryLowLevel = 11;
	    }
	    else {
	    	try {
	    		tryLowLevel = Integer.parseInt(args[4]);
	    	} catch (Exception e) {
	    		tryLowLevel = 2;
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
    		        
    		        Pointing p = Elem.degrees2Point(Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));
    		        
    		        long littlePix = HealpixProc.ang2pixNest(highLevel.value(), p);
    		        
    		        Vec3 v = new Vec3(p);
    		        
    		        long [] neighbours = HealpixProc.neighboursNest(highLevel.value(), littlePix);
    		        HashSet<Long> bigPixes = new HashSet<>();
    		        int difference = 2 * (highLevel.value() - lowLevel.value());
    		        bigPixes.add(littlePix >>> difference);
    		        for (long neighbour : neighbours) {
    					bigPixes.add(neighbour >>> difference);
    				}

    		        //System.out.println(littlePix);
    				for (Long pix : bigPixes) {
    					//System.out.println(bigPixes);
    					elems.add(new Tuple2<Long, Elem>(pix, new Elem(id, p)));
    				}
    				
    			} catch (Exception e) {
    				//System.out.println(e.getMessage());
    			}
    			return elems.iterator();
    		}
    	};
    	
    	PairFlatMapFunction<String, Long, Elem> parse = new PairFlatMapFunction<String, Long, Elem>() {
    		
    		@Override
    		public Iterator<Tuple2<Long, Elem>> call(String arg0) throws Exception {
    			ArrayList<Tuple2<Long, Elem>> elems = new ArrayList<>();
    			//final double ToRadians360 = 2 * Math.PI;
        		//final double ToRadians90 = 0.5 * Math.PI;
    			try {
    				String [] parts = arg0.split(",");
    		        long id = Long.parseLong(parts[0]);
    		        
    		        Pointing p = Elem.degrees2Point(Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));
    		        
    		        /*double r1 = (random.nextFloat() - 0.5) * Math.toRadians(1.0/3600);
    		        double r2 = (random.nextFloat() - 0.5) * Math.toRadians(1.0/3600);
    		        double n = Math.sqrt(r1 * r1 + r2 * r2);
    		        if (n < Math.toRadians(1.0/3600)) {
    		        	n = 1;
    		        }
    		        p.phi += r1 / n;
    		        p.theta += r2 / n;*/
    		       
    		        elems.add(new Tuple2<Long, Elem>(HealpixProc.ang2pixNest(lowLevel.value(), p), new Elem(id, p)));
    			} catch (Exception e) {
    				//System.out.println(e.getMessage());
    			}
    			return elems.iterator();
    		}
    	};
	
	    if(tryFilter /*&& xPart < 8*/) 
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
	    		        
	    		        Pointing p = Elem.degrees2Point(Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));
	    		       
	    		        long pix = HealpixProc.ang2pixNest(lowLevel.value(), p);
		    		       
	    		        if (xKeys.value().contains(pix)) {
	    		        	elems.add(new Tuple2<Long, Elem>(pix, new Elem(id, p)));
	    		        }
	    			} catch (Exception e) {
	    				//System.out.println(e.getMessage());
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
		
		System.out.println(partitions);
		
		/*xElems.mapValues(var -> 1L).reduceByKey( (a, b) -> a + b)
		.mapToPair((Tuple2<Long, Long> arg0) -> new Tuple2<Long, Long>(arg0._2(), arg0._1()))
		.mapValues(var -> 1L).reduceByKey((a, b) -> a + b).sortByKey().coalesce(1).saveAsTextFile("C:///Data/Crossmatch/StatX");
		
		yElems.mapValues(var -> 1L).reduceByKey( (a, b) -> a + b)
		.mapToPair((Tuple2<Long, Long> arg0) -> new Tuple2<Long, Long>(arg0._2(), arg0._1()))
		.mapValues(var -> 1L).reduceByKey((a, b) -> a + b).sortByKey().coalesce(1).saveAsTextFile("C:///Data/Crossmatch/StatY");*/
		
		JavaPairRDD<Long, Iterable<Elem>> xGroup = xElems.groupByKey(partitions);
	    JavaPairRDD<Long, Iterable<Elem>> yGroup = yElems.groupByKey(partitions);

	    JavaPairRDD<Long, Tuple2<Iterable<Elem>, Iterable<Elem>>> z = xGroup.join(yGroup, partitions);

	    
	    
	    PairFlatMapFunction<Tuple2<Long, Tuple2<Iterable<Elem>,Iterable<Elem>>>, Long, Tuple2<Long,Double>> 
		nearestFast = new PairFlatMapFunction<Tuple2<Long, Tuple2<Iterable<Elem>, Iterable<Elem>>>, Long, Tuple2<Long, Double>>() {
			@Override
			public Iterator<Tuple2<Long, Tuple2<Long, Double>>> call(
					Tuple2<Long, Tuple2<Iterable<Elem>, Iterable<Elem>>> pixCat1Cat2) throws Exception {
						final double maxDist = Math.toRadians(1.0/3600.0); //2 * Math.sin(Math.toRadians(1.0/3600.0) * 0.5);
						final int highPix = highLevel.value();
						
						Iterable<Elem> iterCat1 = pixCat1Cat2._2()._1(), cat2 = pixCat1Cat2._2()._2();
						ArrayList<Tuple2<Long, Tuple2<Long, Double>>> result = new ArrayList<>();
						
						ArrayList<Elem> cat1 = new ArrayList<>();
						ArrayList<Elem> matches = new ArrayList<>();
						ArrayList<Double> dists = new ArrayList<>();
						
						HashMultimap<Long, Integer> hash = HashMultimap.create();
						
						int index = 0;
						for (Elem e : iterCat1) {
							cat1.add(e);
							matches.add(null);
							dists.add(maxDist);
							long key = HealpixProc.ang2pixNest(highPix, e.p());
							hash.put(key, index++);
						}
						if (index > 1000000) {
							System.out.println(index + ", " + pixCat1Cat2._1());
						}
						
						for (Elem e2 : cat2) {
							long e2Pix = HealpixProc.ang2pixNest(highPix, e2.p());
							long [] neighbours = HealpixProc.neighboursNest(highPix, e2Pix);
							long[] cells = Arrays.copyOf(neighbours, neighbours.length + 1);
							cells[neighbours.length] = e2Pix;
							
							double minDist = maxDist;
							int nearestIndex = -1;
							//int counter = 0;
							for (long c : cells) {
								if (!hash.containsKey(c)) {
									continue;
								}
								for (int i : hash.get(c)) {
									double dist = e2.dist(cat1.get(i));
									if (dist < minDist) {
										minDist = dist;
										nearestIndex = i;
									}
								}
								//counter += hash.get(c).size();
							}
							/*if (counter > 2) {
								System.out.println(counter);
							}*/
							
							if (nearestIndex == -1) {
								continue;
							}
							
							if (minDist < dists.get(nearestIndex)) {
								dists.set(nearestIndex,  minDist);
								matches.set(nearestIndex, e2);
							}
						}
						
						for (int i = 0; i < dists.size(); ++i) {
							if (matches.get(i) == null) { continue; }
							result.add(new Tuple2<Long, Tuple2<Long, Double>>(cat1.get(i).id, new Tuple2<Long, Double>(matches.get(i).id, dists.get(i) / maxDist)));
						}
						
						return result.iterator();
					}
		};
	    
	    
	    
	    JavaPairRDD<Long, Tuple2<Long, Double>> w = z.flatMapToPair(nearestFast);
	    
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    w = w.sortByKey().coalesce(1);
	    
	    w.saveAsTextFile(args[2]);
	    
	    long tEnd = System.currentTimeMillis();
	    long tDelta = tEnd - tStart;
	    double elapsedSeconds = tDelta / 1000.0;
	    
	    System.out.println("Íå çàáóäü ïîñìîòðåòü ÷èñëî îáúåêòîâ!\nÑÎÏÎÑÒÀÂËÅÍÎ çà " + elapsedSeconds);
	    //sc.close();
		return 0;
	}
}
