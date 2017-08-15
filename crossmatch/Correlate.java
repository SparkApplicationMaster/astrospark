package crossmatch;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.calcite.util.Static;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.random.*;

import healpix.essentials.HealpixBase;
import healpix.essentials.HealpixProc;
import healpix.essentials.Moc;
import healpix.essentials.MocFitsIO;
import healpix.essentials.MocStringIO;
import healpix.essentials.Pointing;
import healpix.essentials.RangeSet;
import scala.Tuple2;


public class Correlate {
	static long [] sumArray (long [] arg0, long [] arg1) {
		for (int i = 0; i < arg0.length; ++i) {
			arg0[i] += arg1[i];
		}
		return arg0;
	}
	
	static Iterator<Tuple2<Long, Integer>> generateCatalog(Long bigCell, long size, int lowLevel, int highLevel) throws Exception {
		ArrayList<Tuple2<Long, Integer>> cells = new ArrayList<>();
		final long highPixCount = HealpixBase.order2Npix(highLevel);
		int difference = 2 * (highLevel - lowLevel);
		final int highPixInLow = 1 << difference;
		PoissonSampler<Integer> poisson = new PoissonSampler<Integer>((float)size / highPixCount);
		
		for (int i = 0; i < highPixInLow; ++i) {
			int objectCount = poisson.sample();
			if (objectCount != 0) {
				long smallCell = HealpixProc.nest2ring(highLevel, (bigCell << difference) + i);
				cells.add(new Tuple2<>(smallCell, objectCount));
			}
		}
		
		return cells.iterator();
    };
    
    static Iterator<Tuple2<Long, Pointing>> parseCatalog(String inputString, int highLevel, int idColumn, int raColumn, int decColumn) throws Exception {
    	ArrayList<Tuple2<Long, Pointing>> cells = new ArrayList<>();
		try {
			String [] parts = inputString.split(",");
			long id = -1;
			if (idColumn >= 0) {
				Long.parseLong(parts[idColumn]);
			}
			
	        Pointing p = Elem.degrees2Point(Double.parseDouble(parts[raColumn]), Double.parseDouble(parts[decColumn]));
	        cells.add(new Tuple2<Long, Pointing>(id, p));
		} catch (Exception e) {
			System.out.println("exception: " + inputString + ", " + e.getMessage());
		}
		return cells.iterator();
	}
	
	public static int run(String [] args, SparkConf sparkConf) throws Exception {
		if (args.length < 3) {
	      System.err.println("Usage: Correlate <catalog> <healpix> <size>");
	      return 1;
		}
		
		/*try {
			FileUtils.deleteDirectory(new File(args[1]));
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		
		sparkConf.setAppName("Correlate");
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
    		//System.out.println(st);
    		radiusesLocal[i] = Math.toRadians(st);
    	}
	    
	    final Broadcast<Integer> lowLevel = sc.broadcast(Integer.parseInt(args[1]));
	    final Broadcast<Integer> highLevel = sc.broadcast(Integer.parseInt(args[2]));
	    final Broadcast<double []> radiusesBroad = sc.broadcast(radiusesLocal);
	    
	    /*String textMaskLocal = FileUtils.readFileToString(new File(args[4]));// sc.wholeTextFiles(args[4]).collect().get(0)._2();
	    
		Moc maskLocal = MocFitsIO.mocFromFits(IOUtils.toInputStream(textMaskLocal));
		final Broadcast<String> jsonMask = sc.broadcast(MocStringIO.mocToStringJSON(maskLocal));
		final Broadcast<Integer> maskFullSize = sc.broadcast(Elem.mocToArray(maskLocal, highLevel.value()).length);*/
		
	    int partitions = (int) HealpixProc.order2Npix(lowLevel.value());
	    final Broadcast<Integer> partitionsBroad = sc.broadcast(partitions);
		JavaRDD<String> dataLines = sc.textFile(args[0]);
	    if (dataLines.getNumPartitions() < partitions) {
	    	dataLines = dataLines.repartition(partitions);
	    }
	    partitions = dataLines.getNumPartitions();
		
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
	    
	    JavaRDD<String> randLines = sc.textFile("file:///C://Data/Crossmatch/sdssmock.csv");
	    
	    PairFlatMapFunction<Long, Long, Integer> generateData = new PairFlatMapFunction<Long, Long, Integer>() {

			@Override
			public Iterator<Tuple2<Long, Integer>> call(Long arg0) throws Exception {
				return generateCatalog(arg0, dataSize.value(), lowLevel.value(), highLevel.value());
			}
		};
	    
	    PairFlatMapFunction<Long, Long, Integer> generateRand = new PairFlatMapFunction<Long, Long, Integer>() {

			@Override
			public Iterator<Tuple2<Long, Integer>> call(Long arg0) throws Exception {
				return generateCatalog(arg0, randSize.value(), lowLevel.value(), highLevel.value());
			}
		};
		
		/*PairFlatMapFunction<Long, Long, Integer> generateMask = new PairFlatMapFunction<Long, Long, Integer>() {

			@Override
			public Iterator<Tuple2<Long, Integer>> call(Long arg0) throws Exception {
				ArrayList<Tuple2<Long, Integer>> cells = new ArrayList<>();
				
				Moc bigCellMoc = new Moc();
				bigCellMoc.addPixel(lowLevel.value(), arg0);
				Moc mocMask = MocStringIO.mocFromString(jsonMask.value());

				long [] mask = Elem.mocToArray(mocMask.intersection(bigCellMoc), highLevel.value());
				final double maskWeightThisPixel = (double)mask.length / maskFullSize.value();
				final long current = (long)mask.length * dataSize.value();
				final long currentRandSize = current / maskFullSize.value();
				int [] randCat = new int[mask.length];
				Arrays.fill(randCat, 0);
				Random random = new Random(System.currentTimeMillis());
				for(int i = 0; i < currentRandSize; ++i) {
					int cell = random.nextInt(mask.length);
					randCat[cell]++;
				}
				
				for (int i = 0; i < mask.length; ++i) {
					if (randCat[i] != 0) {
						long cellNum = HealpixProc.nest2ring(highLevel.value(), mask[i]);
						cells.add(new Tuple2<Long, Integer>(cellNum, randCat[i]));
					}
				}
				
				return cells.iterator();
			}
		};*/
		
		PairFlatMapFunction<String, Long, Pointing> parseData = new PairFlatMapFunction<String, Long, Pointing>() {
			@Override
			public Iterator<Tuple2<Long, Pointing>> call(String arg0) throws Exception {
				return parseCatalog(arg0, highLevel.value(), 0, 1, 2);
			}
		};
		
		PairFlatMapFunction<String, Long, Pointing> parseRand = new PairFlatMapFunction<String, Long, Pointing>() {
			@Override
			public Iterator<Tuple2<Long, Pointing>> call(String arg0) throws Exception {
				return parseCatalog(arg0, highLevel.value(), -1, 0, 1);
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
					
					RangeSet neighbours =  HealpixProc.queryDiscInclusiveRing(lowLevel.value(), p, radiuses[0], 8);
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
				
				int [] cat1 = new int[(int) HealpixProc.order2Npix(highLevel.value())];
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

				
				Arrays.fill(cat1, 0);
				
				for (Tuple2<Long, Integer> entry : pixCat1Cat2._2()._1()) {
					cat1[entry._1().intValue()] += entry._2();
					//sum += entry._2();
				}
				
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
						for (Long q : query.difference(smallerQuery).toArray()) {
							sums[i - 1] += v1 * cat1[q.intValue()];
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
		
		Function2<long[], long[], long[]> summarize = new Function2<long[], long[], long[]>() {
			@Override
			public long[] call(long[] arg0, long[] arg1) throws Exception {
				return sumArray(arg0, arg1);
			}
		};
		
		Function2<Integer, Integer, Integer> intSum = new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		};
		
		PairFunction<Tuple2<Long, Pointing>, Long, Integer> objectToCell = new PairFunction<Tuple2<Long,Pointing>, Long, Integer>() {
			@Override
			public Tuple2<Long, Integer> call(Tuple2<Long, Pointing> arg0) throws Exception {
				long littlePix = HealpixProc.ang2pixRing(highLevel.value(), arg0._2());
		        return new Tuple2<Long, Integer>(littlePix, 1);
			}
		};
		
		JavaRDD<Long> randBigCells = sc.parallelize(lowCells, partitions);
		
		JavaPairRDD<Long, Iterable<Tuple2<Long, Integer>>> dataElems = 
				//dataLines.flatMapToPair(parseData).mapToPair(objectToCell).reduceByKey(intSum)
				randBigCells.flatMapToPair(generateData)
				.mapPartitionsToPair(borderize).groupByKey(partitions);
		
		
		JavaPairRDD<Long, Iterable<Tuple2<Long, Integer>>> randElems = 
				//randLines.flatMapToPair(parseRand).mapToPair(objectToCell).reduceByKey(intSum)
				randBigCells.flatMapToPair(generateRand)
				.mapPartitionsToPair(borderize).groupByKey(partitions);
		

		
	    Function<Tuple2<Long, Integer>, String> cellToObject = new Function<Tuple2<Long, Integer>, String>() {

			@Override
			public String call(Tuple2<Long, Integer> arg0) throws Exception {
				Pointing p = HealpixProc.pix2angRing(highLevel.value(), arg0._1());
				return (360 - Math.toDegrees(p.phi)) + "," + (90 - Math.toDegrees(p.theta)) + "," + arg0._2();
			}
		};
		
		Function<Tuple2<Long, Integer>, Long> cellToCount = new Function<Tuple2<Long, Integer>, Long>() {

			@Override
			public Long call(Tuple2<Long, Integer> arg0) throws Exception {
				return (long)arg0._2();
			}
		};
		
		try {
		FileUtils.deleteDirectory(new File("Crossmatch/masked"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		/*randBigCells.flatMapToPair(generateMask).map(cellToPoint).coalesce(1).saveAsTextFile("Crossmatch/masked");
		System.out.println("size: " + randBigCells.flatMapToPair(generateMask).map(cellToCount).reduce(new Function2<Long, Long, Long>() {

			@Override
			public Long call(Long arg0, Long arg1) throws Exception {
				return arg0 + arg1;
			}
		}));*/
		
	    JavaRDD<long []> dataData = dataElems.join(dataElems).flatMap(correlate);
	    long [] ddSum = dataData.reduce(summarize);
	    //System.out.println(resultToString(ddSum));

		JavaRDD<long []> dataRand = dataElems.join(randElems).flatMap(correlate);
	    long [] drSum = dataRand.reduce(summarize);
	    //System.out.println(resultToString(drSum));
		
		JavaRDD<long []> randRand = randElems.join(randElems).flatMap(correlate);
		long [] rrSum = randRand.reduce(summarize);
		//System.out.println(resultToString(rrSum));
	    
	    double [] correlation = new double[ddSum.length];
	    
	    for (int i = 0; i < ddSum.length; ++i) {
	    	correlation[i] = (double)(((double)(rSize) / dSize) * ((double)(rSize) / dSize) * ddSum[i]  - 2 * (double)(rSize) / dSize * drSum[i] + rrSum[i]) / rrSum[i];
	    }
	    System.out.println(resultToString(ddSum));
	    System.out.println(resultToString(drSum));
	    System.out.println(resultToString(rrSum));
	    System.out.println(resultToString(correlation));
	    //System.out.println("DataSize = " + dSize + ", RandSize = " + rSize + ", Level = " + highLevel.value());

		//dataLines.sample(false, 100050.0 / dataLines.count()).coalesce(1).saveAsTextFile("Crossmatch/galex_100k");
	    
	    long tEnd = System.currentTimeMillis();
	    long tDelta = tEnd - tStart;
	    double elapsedSeconds = tDelta / 1000.0;
	    //System.out.println("DataSize = " + dSize + ", RandSize = " + rSize + ", LowLevel = " + lowLevel.value() + ", HighLevel = " + highLevel.value() + ", Time = " + elapsedSeconds);	
	    System.out.println("DataSize,RandSize,LowLevel,HighLevel,Time");
	    System.out.println(dSize + "," + rSize + "," + lowLevel.value() + "," + highLevel.value() + "," + elapsedSeconds);

	    //System.out.println("Не забудь посмотреть лог!");
	    //sc.close();
		return 0;
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
