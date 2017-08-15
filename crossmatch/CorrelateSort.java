package crossmatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import healpix.essentials.HealpixProc;
import healpix.essentials.MocQuery;
import healpix.essentials.Pointing;
import healpix.essentials.RangeSet;
import scala.Tuple2;

public class CorrelateSort {
	static long [] sumArray (long [] arg0, long [] arg1) {
		for (int i = 0; i < arg0.length; ++i) {
			arg0[i] += arg1[i];
		}
		return arg0;
	}
	
	static long [] diffArray (long [] arg0, long [] arg1) {
		for (int i = 0; i < arg0.length; ++i) {
			arg0[i] -= arg1[i];
		}
		return arg0;
	}
	
	static boolean arraysEqual (long [] arg0, long [] arg1) {
		boolean equal = true;
		for (int i = 0; i < arg0.length; ++i) {
			equal = equal && (arg0[i] == arg1[i]);
		}
		return equal;
	}
	
	final static int factor = 8;
	
	static int binSearch(ArrayList<Elem> a, Elem x, final double maxDist) {
		for (int i = 0, j = a.size(); i < j; ) {
			int m = (i + j) / 2;
			double dist = a.get(m).dummyFastDist(x);
			if (dist <= -maxDist) {
				i = m + 1;
			} else if (dist > maxDist) {
				j = m;
			} else {
				return m;
			}
		}
		return -1;
	}

	public static int run(String [] args, SparkConf sparkConf) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: Correlate <catalog> <healpix> <size>");
			return 1;
		}
		
		/*try {
			FileUtils.deleteDirectory(new File(args[1]));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		sparkConf.setAppName("CorrelateSort");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.hadoopConfiguration().setLong("fs.local.block.size", 256 * 1024 * 1024);
		sc.setLogLevel("ERROR");

		long tStart = System.currentTimeMillis();
    	
		final Broadcast<Double> minRadius = sc.broadcast(1.0);
		final Broadcast<Double> maxRadius = sc.broadcast(10.0);
    	
    	final Broadcast<Integer> measureCount = sc.broadcast(10);
	    final double [] radiusesLocal = new double[measureCount.value()];
	    for (int i = 0; i < measureCount.value(); i++) {
    		double st = (double)((measureCount.value() - i - 1 + minRadius.value()) * measureCount.value()) / (maxRadius.value() + 1 - minRadius.value());//Math.pow(10, 2.0 * (double)(n - i - 1) / n - 1.0);
    		//System.out.println(st + ", " + Math.toRadians(st));
    		radiusesLocal[i] = Math.toRadians(st);
    	}
	    
	    final Broadcast<Integer> lowLevel = sc.broadcast(Integer.parseInt(args[1]));
	    final Broadcast<Integer> highLevel = sc.broadcast(Integer.parseInt(args[2]));
	    final Broadcast<double []> radiusesBroad = sc.broadcast(radiusesLocal);
	    
	    int partitions = (int) HealpixProc.order2Npix(lowLevel.value());
	    final Broadcast<Integer> partitionsBroad = sc.broadcast(partitions);
		/*JavaRDD<String> dataLines = sc.textFile(args[0]);
	    if (dataLines.getNumPartitions() < partitions) {
	    	dataLines = dataLines.repartition(partitions);
	    }
	    partitions = dataLines.getNumPartitions();*/
		
	    long dSize = Long.parseLong(args[3]);// dataLines.count();
	    long rSize = /*randLines.count();*/ dSize * 10;
	    final Broadcast<Long> dataSize = sc.broadcast(dSize);
	    final Broadcast<Long> randSize = sc.broadcast(rSize);
	    
	    ArrayList<Long> lowCells = new ArrayList<>();
	    try {
			for (long i = 0; i < HealpixProc.order2Npix(lowLevel.value()); ++i) {
				lowCells.add(i);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	    
	    //JavaRDD<String> randLines = sc.textFile(args[2]);
	    
	    PairFlatMapFunction<Long, Long, Integer> generateData = new PairFlatMapFunction<Long, Long, Integer>() {

			@Override
			public Iterator<Tuple2<Long, Integer>> call(Long arg0) throws Exception {
				ArrayList<Tuple2<Long, Integer>> cells = new ArrayList<>();
				final long lowPixCount = HealpixProc.order2Npix(lowLevel.value());
				
				final long currentRandSize = dataSize.value() / lowPixCount + (arg0 < dataSize.value() % lowPixCount ? 1 : 0);
				int difference = 2 * (highLevel.value() - lowLevel.value());
				final int highPixInLow = 1 << difference;
				int [] randCat = new int[highPixInLow];
				Arrays.fill(randCat, 0);
				Random random = new Random(System.currentTimeMillis());
				for(int i = 0; i < currentRandSize; ++i) {
					int cell = random.nextInt(highPixInLow);
					randCat[cell]++;
				}
				
				for (int i = 0; i < highPixInLow; ++i) {
					if (randCat[i] != 0) {
						long cellNum = HealpixProc.nest2ring(highLevel.value(), (long)i + (arg0 << difference));
						cells.add(new Tuple2<Long, Integer>(cellNum, randCat[i]));
					}
				}

				return cells.iterator();
			}
		};
	    
	    PairFlatMapFunction<Long, Long, Integer> generateRand = new PairFlatMapFunction<Long, Long, Integer>() {

			@Override
			public Iterator<Tuple2<Long, Integer>> call(Long arg0) throws Exception {
				ArrayList<Tuple2<Long, Integer>> cells = new ArrayList<>();
				final long lowPixCount = HealpixProc.order2Npix(lowLevel.value());
				
				final long currentRandSize = randSize.value() / lowPixCount + (arg0 < randSize.value() % lowPixCount ? 1 : 0);
				int difference = 2 * (highLevel.value() - lowLevel.value());
				final int highPixInLow = 1 << difference;
				int [] randCat = new int[highPixInLow];
				Arrays.fill(randCat, 0);
				Random random = new Random(System.currentTimeMillis());
				for(int i = 0; i < currentRandSize; ++i) {
					int cell = random.nextInt(highPixInLow);
					randCat[cell]++;
				}
				
				for (int i = 0; i < highPixInLow; ++i) {
					if (randCat[i] != 0) {
						long cellNum = HealpixProc.nest2ring(highLevel.value(), (long)i + (arg0 << difference));
						cells.add(new Tuple2<Long, Integer>(cellNum, randCat[i]));
					}
				}
				
				return cells.iterator();
			}
		};
	    
	    PairFlatMapFunction<Iterator<String>,Long,Long> parseData = 
	    		new PairFlatMapFunction<Iterator<String>, Long, Long>() {
			@Override
			public Iterator<Tuple2<Long, Long>> call(Iterator<String> arg0) throws Exception {
				ArrayList<Tuple2<Long, Long>> cells = new ArrayList<>();
				HashMap<Long, Long> data = new HashMap<>();
				
				while (arg0.hasNext()) {
					String s = "";
					try {
						s = arg0.next();
	    				String [] parts = s.split(",");
	    		        
	    		        Pointing p = Elem.degrees2Point(Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));
	    		        long littlePix = HealpixProc.ang2pixRing(highLevel.value(), p);
	    		        
	    		        data.put(littlePix, data.containsKey(littlePix) ? 1 + data.get(littlePix) : 1);
	    		        
	    			} catch (Exception e) {
	    				System.out.println("exception: " + s + ", " + e.getMessage());
	    			}
				}
				for (Entry<Long, Long> entry : data.entrySet()) {
					cells.add(new Tuple2<Long, Long>(entry.getKey(), entry.getValue()));
				}
				return cells.iterator();
			}
	    };
	    
	    PairFlatMapFunction<Iterator<String>,Long,Long> parseRand = 
	    		new PairFlatMapFunction<Iterator<String>, Long, Long>() {
			@Override
			public Iterator<Tuple2<Long, Long>> call(Iterator<String> arg0) throws Exception {
				ArrayList<Tuple2<Long, Long>> cells = new ArrayList<>();
				HashMap<Long, Long> data = new HashMap<>();
				
				while (arg0.hasNext()) {
					String s = "";
					try {
						s = arg0.next();
	    				String [] parts = s.split(",");
	    		        
	    		        Pointing p = Elem.degrees2Point(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]));
	    		        long littlePix = HealpixProc.ang2pixRing(highLevel.value(), p);
	    		        
	    		        data.put(littlePix, data.containsKey(littlePix) ? 1 + data.get(littlePix) : 1);
	    		        
	    			} catch (Exception e) {
	    				System.out.println("exception: " + s + ", " + e.getMessage());
	    			}
				}
				
				for (Entry<Long, Long> entry : data.entrySet()) {
					cells.add(new Tuple2<Long, Long>(entry.getKey(), entry.getValue()));
				}
				return cells.iterator();
			}
	    };
    	
    	PairFlatMapFunction<Iterator<Tuple2<Long, Integer>>, Long, Tuple2<Long, Integer>> borderize = 
    			new PairFlatMapFunction<Iterator<Tuple2<Long, Integer>>, Long, Tuple2<Long, Integer>>() {
			@Override
			public Iterator<Tuple2<Long, Tuple2<Long, Integer>>> call(Iterator<Tuple2<Long, Integer>> arg0) throws Exception {
				
				ArrayList<Tuple2<Long, Tuple2<Long, Integer>>> cells = new ArrayList<>();
				double [] radiuses = radiusesBroad.value();
				while (arg0.hasNext()) {
					Tuple2<Long, Integer> entry = arg0.next();
					Pointing p = HealpixProc.pix2angRing(highLevel.value(), entry._1());

					//HashSet<Long> bigPixes = new HashSet<>();
					
					RangeSet neighbours =  HealpixProc.queryDiscInclusiveRing(lowLevel.value(), p, radiuses[0], factor);
							/*HealpixProc.queryDiscRing(highLevel.value(), p, radiuses[0])
							.difference(HealpixProc.queryDiscRing(highLevel.value(), p, radiuses[0] - pixelRadiuses.value()[highLevel.value()] * 4));
					for (long neighbour : neighbours.toArray()) {
			        	bigPixes.add(Elem.pixToLowerLevel(neighbour, highLevel.value(), lowLevel.value()));
					}
					bigPixes.add(HealpixProc.ang2pixRing(lowLevel.value(), p));
			        
					for (Long pix : bigPixes) {*/
					for (long pix : neighbours.toArray()) {
						cells.add(new Tuple2<Long, Tuple2<Long, Integer>>(pix, entry));
					}
				}
    			return cells.iterator();
			}
		};
		
		FlatMapFunction<Tuple2<Long, Tuple2<Iterable<Tuple2<Long, Integer>>, Iterable<Tuple2<Long, Integer>>>>, long []> correlate =
				new FlatMapFunction<Tuple2<Long, Tuple2<Iterable<Tuple2<Long, Integer>>, Iterable<Tuple2<Long, Integer>>>>, long []>() {
			@Override
			public Iterator<long []> call(Tuple2<Long, Tuple2<Iterable<Tuple2<Long, Integer>>, Iterable<Tuple2<Long, Integer>>>> pixCat1Cat2) throws Exception {
				final int highPix = highLevel.value();
				double [] radiuses = radiusesBroad.value();
				
				ArrayList<long []> result = new ArrayList<>();
				
				HashMap<Long, Integer> cat1 = new HashMap<>();
				HashMap<Long, Integer> cat2 = new HashMap<>();
				
				long sum = 0;
				for (Tuple2<Long, Integer> entry : pixCat1Cat2._2()._2()) {
					if (Elem.pixToLowerLevel(entry._1(), highLevel.value(), lowLevel.value()) == pixCat1Cat2._1()) {
						cat2.put(entry._1(), entry._2() + (cat2.containsKey(entry._1()) ? cat2.get(entry._1()) : 0));
						//sum += entry._2();
					}
				}
				
				if (cat2.isEmpty()) {
					return result.iterator();
				}

				
				for (Tuple2<Long, Integer> entry : pixCat1Cat2._2()._1()) {
					cat1.put(entry._1(), entry._2() + (cat1.containsKey(entry._1()) ? cat1.get(entry._1()) : 0));
					//sum += entry._2();
				}
				
				int [] cat1a = new int[(int) HealpixProc.order2Npix(highLevel.value())];
				//int [] cat2a = new int[cat1a.length];
				Arrays.fill(cat1a, 0);
				//Arrays.fill(cat2a, 0);
				
				for (Entry<Long, Integer> e : cat1.entrySet()) {
					cat1a[e.getKey().intValue()] = e.getValue();
				}
				
				/*for (Entry<Long, Integer> e : cat2.entrySet()) {
					cat2a[e.getKey().intValue()] = e.getValue();
				}*/
				
				
				long[] sums = new long[radiuses.length];
				Arrays.fill(sums, 0);
				
				//long tt1 = 0;
				//long t1 = System.currentTimeMillis();
				for (Entry<Long, Integer> e : cat2.entrySet()) {
					Pointing ptg = HealpixProc.pix2angRing(highPix, e.getKey());
					long v1 = e.getValue();
					
					RangeSet query = HealpixProc.queryDiscRing(highPix, ptg, radiuses[0]);
					for (int i = 1; i < radiuses.length; ++i) {
						RangeSet smallerQuery = HealpixProc.queryDiscRing(highPix, ptg, radiuses[i]);
						for (Long q : query.difference(smallerQuery).toArray())
						{
							/*Long v2 = cat1.get(q);
							if (v2 != null) {
								sums[i - 1] += v1 * v2;
							}*/
							sums[i - 1] += v1 * cat1a[q.intValue()];
						}
						query = smallerQuery;
					}
				}
				
				/*int[] sums = new int [2 * partitionsBroad.value() + 2];
				Arrays.fill(sums, 0);
				sums[pixCat1Cat2._1().intValue()] = cat1.size();
				sums[partitionsBroad.value()] = 0;
				sums[pixCat1Cat2._1().intValue() + 1 + partitionsBroad.value()] = cat2.size();*/
				
				//System.out.println(sum + ", " + sums[0]);
				result.add(sums);
				return result.iterator();
			}
		};
		
	    PairFlatMapFunction<Tuple2<Long, Tuple2<Long, Integer>>, Long, Elem> cellToElem = new PairFlatMapFunction<Tuple2<Long, Tuple2<Long, Integer>>, Long, Elem>() {

			@Override
			public Iterator<Tuple2<Long, Elem>> call(Tuple2<Long, Tuple2<Long, Integer>> arg0) throws Exception {
				ArrayList<Tuple2<Long, Elem>> cells = new ArrayList<>();
				for (int i = 0; i < arg0._2()._2(); ++i) {
					Pointing p = HealpixProc.pix2angRing(highLevel.value(), arg0._2()._1());
					cells.add(new Tuple2<Long, Elem>(arg0._1(), new Elem(arg0._2()._1(), p)));
				}
				return cells.iterator();
			}
		};
		
		PairFlatMapFunction<Iterator<String>, Long, Elem> parseElem = new PairFlatMapFunction<Iterator<String>, Long, Elem>() {
    		@Override
    		public Iterator<Tuple2<Long, Elem>> call(Iterator<String> arg0) throws Exception {
    			ArrayList<Tuple2<Long, Elem>> elems = new ArrayList<>();
    			while (arg0.hasNext()) {
	    			try {
	    				String e = arg0.next();
	    				String [] parts = e.split(",");
	    		        long id = Long.parseLong(parts[0]);
	    		        
	    		        Pointing p = new Pointing(Math.toRadians(90 - Double.parseDouble(parts[2])),
	    		        		Math.toRadians(360 - Double.parseDouble(parts[1])));
	    		        
	    		        long littlePix = HealpixProc.ang2pixNest(highLevel.value(), p);
	    		        
	    				elems.add(new Tuple2<Long, Elem>(0L, new Elem(id, p)));
	    				
	    			} catch (Exception e) {
	    				System.out.println(e.getMessage());
	    			}
    			}
    			return elems.iterator();
    		}
    	};
    	
    	PairFlatMapFunction<Iterator<Tuple2<Long, Elem>>, Long, Elem> borderizeElem = 
    			new PairFlatMapFunction<Iterator<Tuple2<Long, Elem>>, Long, Elem>() {
			@Override
			public Iterator<Tuple2<Long, Elem>> call(Iterator<Tuple2<Long, Elem>> arg0) throws Exception {
				
				ArrayList<Tuple2<Long, Elem>> cells = new ArrayList<>();
				double [] radiuses = radiusesBroad.value();
				while (arg0.hasNext()) {
					Elem entry = arg0.next()._2();
					Pointing p = entry.p();
					//HashSet<Long> bigPixes = new HashSet<>();
					
					RangeSet neighbours =  HealpixProc.queryDiscInclusiveRing(lowLevel.value(), p, radiuses[0], factor);
							/*HealpixProc.queryDiscRing(highLevel.value(), p, radiuses[0])
							.difference(HealpixProc.queryDiscRing(highLevel.value(), p, radiuses[0] - pixelRadiuses.value()[highLevel.value()] * 4));
					for (long neighbour : neighbours.toArray()) {
			        	bigPixes.add(Elem.pixToLowerLevel(neighbour, highLevel.value(), lowLevel.value()));
					}
					bigPixes.add(HealpixProc.ang2pixRing(lowLevel.value(), p));
			        
					for (Long pix : bigPixes) {*/
					for (long pix : neighbours.toArray()) {
						cells.add(new Tuple2<Long, Elem>(pix, entry));
					}
				}
    			return cells.iterator();
			}
		};
		
		FlatMapFunction<Tuple2<Long, Tuple2<Iterable<Elem>, Iterable<Elem>>>, long []> 
		correlateElem = new FlatMapFunction<Tuple2<Long, Tuple2<Iterable<Elem>, Iterable<Elem>>>, long []>() {
			@Override
			public Iterator<long []> call(Tuple2<Long, Tuple2<Iterable<Elem>, Iterable<Elem>>> pixCat1Cat2) throws Exception {
				final int highPix = highLevel.value();
				double [] radiuses = radiusesBroad.value();
				
				ArrayList<long []> result = new ArrayList<>();
				
				Iterable<Elem> iterCat1 = pixCat1Cat2._2()._1(), iterCat2 = pixCat1Cat2._2()._2();
				
				ArrayList<Elem> cat1 = new ArrayList<>();
				for (Elem e : iterCat1) {
					cat1.add(e);
				}
				
				ArrayList<Elem> cat2 = new ArrayList<>();
				for (Elem e : iterCat2) {
					if (HealpixProc.ang2pixRing(lowLevel.value(), e.p()) == pixCat1Cat2._1()) {
						cat2.add(e);
					}
				}
				
				Collections.sort(cat1, new Comparator<Elem>() {
					@Override
					public int compare(Elem e1, Elem e2) {
						return Double.compare(e1.dummyFastDist(e2), 0);
					}
				});
				
				double maxDist = radiuses[0];
				
				long[] sums = new long [radiuses.length];
				Arrays.fill(sums, 0);
				int x = 0;
				for (Elem e : cat2) {
					//System.out.println(x++);
					int index = binSearch(cat1, e, maxDist);
					if (index < 0) { continue; }
					
					
					for (int i = index; (i >= 0) && (e.dummyFastDist(cat1.get(i)) < maxDist); --i) {
						double dist = e.dist(cat1.get(i));
						
						/*
						x = Math.pow(10, 2.0 * (double)(n - i - 1) / n - 1.0);
						log10(x) = 2.0 * (double)(n - i - 1) / n - 1.0
						log10(x) + 1.0 = 2.0 * (double)(n - i - 1) / n
						16 * (log10(x) + 1.0) = 31 - i
						
						*/
						
						//int ind = 30 - (int)(16 * (1.0 + Math.log10(Math.toDegrees(dist))));
						//System.out.println(dist + ", " + ind);
								//(int) (measureCount.value() - 1 + minRadius.value() + dist * (maxRadius.value() + 1 - minRadius.value()) / measureCount.value());
						//(double)((measureCount.value() - i - 1 + minRadius.value()) * measureCount.value()) / (maxRadius.value() + 1 - minRadius.value());

						/*if (ind >= 0 && ind < radiuses.length) {
							System.out.println(dist + ", " + ind);
							sums[radiuses.length - 1 - ind]++;
						}*/
						for (int j = radiuses.length - 1; j >= 1; --j) {
							if (dist < radiuses[j - 1] && dist >= radiuses[j]) {
								sums[j - 1]++;
								break;
							}
						}
					}
					
					for (int i = index + 1; (i < cat1.size()) &&  (cat1.get(i).dummyFastDist(e) < maxDist); ++i) {
						double dist = e.dist(cat1.get(i));
						
						//int ind = 30 - (int)(16 * (1.0 + Math.log10(Math.toDegrees(dist))));
								//(int) (measureCount.value() - 1 + minRadius.value() + dist * (maxRadius.value() + 1 - minRadius.value()) / measureCount.value());
						/*if (ind >= 0 && ind < radiuses.length) {
							System.out.println(dist + ", " + ind);
							sums[radiuses.length - 1 - ind]++;
						}*/
						for (int j = radiuses.length - 1; j >= 1; --j) {
							if (dist < radiuses[j - 1] && dist >= radiuses[j]) {
								sums[j - 1]++;
								break;
							}
						}
					}
				}
				
				//System.out.println(sum + ", " + sums[0]);
				result.add(sums);
				return result.iterator();
			}
		};
		
		Function2<long[], long[], long[]> summarize = 
				new Function2<long[], long[], long[]>() {
			
			@Override
			public long[] call(long[] arg0, long[] arg1) throws Exception {
				return sumArray(arg0, arg1);
			}
		};
		
		JavaRDD<Long> randBigCells = sc.parallelize(lowCells, partitions);
		JavaPairRDD<Long, Tuple2<Long, Integer>> dataTmp = randBigCells.flatMapToPair(generateData).mapPartitionsToPair(borderize).cache();
		JavaPairRDD<Long, Iterable<Elem>> dataElems = //dataLines.mapPartitionsToPair(parse).mapPartitionsToPair(borderize).groupByKey(partitions);
				//randBigCells.flatMapToPair(generateData)
				dataTmp.flatMapToPair(cellToElem).groupByKey(partitions);
		
		JavaPairRDD<Long, Tuple2<Long, Integer>> randTmp = randBigCells.flatMapToPair(generateRand).mapPartitionsToPair(borderize).cache();
		JavaPairRDD<Long, Iterable<Elem>> randElems = //randLines.mapPartitionsToPair(parseRand)
				//randBigCells.flatMapToPair(generateRand)
				randTmp.flatMapToPair(cellToElem).groupByKey(partitions);
		
	    JavaRDD<long []> dataData = dataElems.join(dataElems).flatMap(correlateElem);
	    long [] ddSum = dataData.reduce(summarize);
	    //System.out.println(resultToString(ddSum));

		JavaRDD<long []> dataRand = dataElems.join(randElems).flatMap(correlateElem);
	    long [] drSum = dataRand.reduce(summarize);
	    //System.out.println(resultToString(drSum));
		
		JavaRDD<long []> randRand = randElems.join(randElems).flatMap(correlateElem);
		long [] rrSum = randRand.reduce(summarize);
		//System.out.println(resultToString(rrSum));
	    
	    /*double [] correlation = new double[ddSum.length];
	    
	    for (int i = 0; i < ddSum.length; ++i) {
	    	correlation[i] = (double)(((double)(rSize) / dSize) * ((double)(rSize) / dSize) * ddSum[i]  - 2 * (double)(rSize) / dSize * drSum[i] + rrSum[i]) / rrSum[i];
	    }*/
	    //System.out.println(resultToString(ddSum));
	    //System.out.println(resultToString(drSum));
	    //System.out.println(resultToString(rrSum));
	    //System.out.println(resultToString(correlation));
	    
	    JavaPairRDD<Long, Iterable<Tuple2<Long, Integer>>> dataCells = //dataLines.mapPartitionsToPair(parse).mapPartitionsToPair(borderize).groupByKey(partitions);
				dataTmp.groupByKey(partitions);
		
		
		JavaPairRDD<Long, Iterable<Tuple2<Long, Integer>>> randCells = //randLines.mapPartitionsToPair(parseRand)
				randTmp.groupByKey(partitions);
		
	    dataData = dataCells.join(dataCells).flatMap(correlate);
	    long [] ddSum1 = dataData.reduce(summarize);
	    //System.out.println(resultToString(ddSum));

		dataRand = dataCells.join(randCells).flatMap(correlate);
		long  [] drSum1 = dataRand.reduce(summarize);
	    //System.out.println(resultToString(drSum));
		
		randRand = randCells.join(randCells).flatMap(correlate);
		long [] rrSum1 = randRand.reduce(summarize);
		//System.out.println(resultToString(rrSum));
	    
	    /*for (int i = 0; i < ddSum1.length; ++i) {
	    	correlation[i] = (double)(((double)(rSize) / dSize) * ((double)(rSize) / dSize) * ddSum1[i]  - 2 * (double)(rSize) / dSize * drSum1[i] + rrSum1[i]) / rrSum1[i];
	    }*/
	    //System.out.println(resultToString(diffArray(ddSum1, ddSum)));
	    //System.out.println(resultToString(diffArray(drSum1, drSum)));
	    //System.out.println(resultToString(diffArray(rrSum1, rrSum)));
	    //System.out.println(resultToString(correlation));

		//dataLines.sample(false, 100050.0 / dataLines.count()).coalesce(1).saveAsTextFile("Crossmatch/galex_100k");
	    
	    long tEnd = System.currentTimeMillis();
	    long tDelta = tEnd - tStart;
	    double elapsedSeconds = tDelta / 1000.0;
	    //System.out.println("DataSize = " + dSize + ", RandSize = " + rSize + ", LowLevel = " + lowLevel.value() + ", HighLevel = " + highLevel.value() + ", Time = " + elapsedSeconds);	
	    //System.out.println("DataSize,RandSize,LowLevel,HighLevel,Time");
	    //System.out.println(dSize + "," + rSize + "," + lowLevel.value() + "," + highLevel.value() + "," + elapsedSeconds);

	    //System.out.println("Не забудь посмотреть лог!");
	    sc.close();
		return arraysEqual(ddSum1, ddSum) && arraysEqual(drSum1, drSum) && arraysEqual(rrSum1, rrSum) ? 0 : 1;
	}
	
	static String resultToString(long [] result) {
		String str = "";
	    for (int i = 1; i < result.length; ++i) {
			str += result[result.length - 1 - i] + (i == result.length - 1 ? "" : ",");
		}
	    return str;
	}
	
	static String resultToString(double [] result) {
		String str = "";
		for (int i = 1; i < result.length; ++i) {
			str += result[result.length - 1 - i] + (i == result.length - 1 ? "" : ",");
		}
	    return str;
	}
}
