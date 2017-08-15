package correlate;

import scala.collection.Iterable
import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.LongMap

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.util.random.PoissonSampler
import org.apache.spark.storage.StorageLevel

import healpix.essentials.HealpixBase
import healpix.essentials.HealpixProc
import healpix.essentials.Pointing
import scala.util.Random
import scala.reflect.ClassTag
import healpix.essentials.RangeSet
import java.io.File
import java.nio.file.Files
import healpix.essentials.HealpixUtils
import scala.collection.mutable.MutableList
import scala.reflect.io.Path
import scala.util.Try
import collection.JavaConverters._
import scala.collection.immutable.TreeMap
import healpix.essentials.Vec3
import java.util.regex.Pattern.Slice
import scala.reflect.io.File
import java.io.OutputStream
import java.io.FileOutputStream
import java.io.PrintWriter
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.mutable.ParMap
import scala.collection.Map

abstract class Binner(protected val minRadiusDeg: Double, protected val maxRadiusDeg: Double, val binCount: Int) extends Serializable {

	val minRadius = math.toRadians(minRadiusDeg);
	val maxRadius = math.toRadians(maxRadiusDeg);

	val correctedMax = straightFunction(maxRadiusDeg);
	val correctedMin = straightFunction(minRadiusDeg);

	val bins = createBins();

	def createBins(): Array[Double] = {
		val tmpBins = Array.fill(binCount + 1) { 0.0 };
		for (i <- 0 to binCount) {
			tmpBins(i) = indexToDist(i);
		}
		return tmpBins;
	}

	def distToIndex(dist: Double): Int = {
		return ((straightFunction(math.toDegrees(dist)) - correctedMin) * binCount / (correctedMax - correctedMin)).toInt;
	}
	def indexToDist(index: Int): Double = {
		return math.toRadians(inverseFunction(correctedMin + (correctedMax - correctedMin) * index.toDouble / binCount));
	}

	def straightFunction(x: Double): Double;
	def inverseFunction(x: Double): Double;

}

object Binner {
	def create(args: HashMap[String, String]): Binner = {
		val binner = args("scale") match {
			case "sqrt" => new SqrtBinner(args("minradius").toDouble, args("maxradius").toDouble, args("bins").toInt);
			case "linear" => new LinearBinner(args("minradius").toDouble, args("maxradius").toDouble, args("bins").toInt);
			case _ => new Log10Binner(args("minradius").toDouble, args("maxradius").toDouble, args("bins").toInt);
		}
		return binner;
	}
}

class LinearBinner(override val minRadiusDeg: Double, override val maxRadiusDeg: Double, override val binCount: Int) extends Binner(minRadiusDeg, maxRadiusDeg, binCount) {

	override def straightFunction(x: Double): Double = {
		return x;
	}

	override def inverseFunction(x: Double): Double = {
		return x;
	}

}

class SqrtBinner(override val minRadiusDeg: Double, override val maxRadiusDeg: Double, override val binCount: Int) extends Binner(minRadiusDeg, maxRadiusDeg, binCount) {

	override def straightFunction(x: Double): Double = {
		return math.sqrt(x);
	}

	override def inverseFunction(x: Double): Double = {
		return x * x;
	}
}

class Log10Binner(override val minRadiusDeg: Double, override val maxRadiusDeg: Double, override val binCount: Int) extends Binner(minRadiusDeg, maxRadiusDeg, binCount) {

	override def straightFunction(x: Double): Double = {
		return math.log10(x);
	}

	override def inverseFunction(x: Double): Double = {
		return math.pow(10, x);
	}
}

class RaDec(val point: Pointing, val weight: Double, val id: Long) {

	def ra = point.phi;
	def dec = point.theta;

	def this(ra: Double, dec: Double, weight: Double = 0.0, id: Long = 0L) = {
		this(ObjectTools.degrees2Point(ra, dec), weight, id)
	}

}

object ObjectTools {
	val degrees2Point = (raDeg: Double, decDeg: Double) => new Pointing(math.toRadians(90 - decDeg), math.toRadians(360 - raDeg));

	def neighboursRing = (lowLevel: Int, point: Pointing, radius: Double, factor: Int) => HealpixProc.queryDiscInclusiveRing(lowLevel, point, radius, factor).toArray();

	def objectToCell(obj: (Long, (Double, Double, Double)), highLevel: Int): (Long, Double) = {
		return (HealpixProc.ang2pixRing(highLevel, new Pointing(obj._2._2, obj._2._1)), obj._2._3);
	}

	def objectsToCells(objs: Iterator[(Long, (Double, Double, Double))], highLevel: Int): Iterator[(Long, Double)] = {
		val cells = new LongMap[Double](_ => 0.0);
		var x = 0;
		for (obj <- objs) {
			val cell = HealpixProc.ang2pixRing(highLevel, new Pointing(obj._2._2, obj._2._1));
			cells(cell) += obj._2._3;
		}
		return cells.iterator;
	}

	def haversine(a: Pointing, b: Pointing): Double = {
		val lat1 = math.Pi / 2 - a.theta;
		val lat2 = math.Pi / 2 - b.theta;
		val dLat = lat1 - lat2;
		val dLon = b.phi - a.phi;

		val d = math.pow(math.sin(dLat / 2), 2) + math.pow(math.sin(dLon / 2), 2) * math.cos(lat1) * math.cos(lat2);
		return 2 * math.asin(math.sqrt(d));
	}

	def generateCatalog(bigCell: Long, size: Long, lowLevel: Int, highLevel: Int): TraversableOnce[(Long, (Double, Double, Double))] = {
		val RAmin = 0.000000001;
		val RAmax = 359.99999999;
		val DECmin = math.toRadians(-89.99999999);
		val DECmax = math.toRadians(89.99999999);

		Random.setSeed((bigCell + 1) * System.currentTimeMillis());
		val cells = new MutableList[(Long, (Double, Double, Double))];
		for (i <- 0L until (size.toDouble / HealpixBase.order2Npix(lowLevel)).toLong) {
			/*cells += Tuple2(bigCell, (math.toRadians(360 - (RAmin + (RAmax - RAmin) * Random.nextDouble)),
				math.toRadians(90 - math.toDegrees(math.asin(math.sin(DECmin) + (math.sin(DECmax) - math.sin(DECmin)) * Random.nextDouble))), 1));*/
			val x = Random.nextDouble() * 2 - 1;
			val y = Random.nextDouble() * 2 - 1;
			val z = Random.nextDouble() * 2 - 1;
			val norm = math.sqrt(x * x + y * y + z * z);
			val p = new Pointing(new Vec3(x / norm, y / norm, z / norm));
			cells += Tuple2(bigCell, (math.atan2(y, x), math.acos(z / norm), 1));
		}
		return cells;
	}

	def generateCatalogAstroML(bigCell: Long, size: Long, lowLevel: Int, highLevel: Int): TraversableOnce[(Long, (Double, Double, Double))] = {

		val RAmin = 140;
		val RAmax = 220;
		val DECmin = math.toRadians(5);
		val DECmax = math.toRadians(45);

		Random.setSeed((bigCell + 1) * System.currentTimeMillis());
		val cells = new MutableList[(Long, (Double, Double, Double))];
		for (i <- 0L until (size.toDouble / HealpixBase.order2Npix(lowLevel)).toLong) {
			cells += Tuple2(bigCell, (math.toRadians(360 - (RAmin + (RAmax - RAmin) * Random.nextDouble)),
				math.toRadians(90 - math.toDegrees(math.asin(math.sin(DECmin) + (math.sin(DECmax) - math.sin(DECmin)) * Random.nextDouble))), 1));
		}
		return cells;
	}

	def parseCatalog(inputString: String, highLevel: Int, raColumn: Int, decColumn: Int, weightColumn: Int = -1, idColumn: Int = -1): TraversableOnce[(Long, (Double, Double, Double))] = {
		val cells = new ArrayBuffer[(Long, (Double, Double, Double))];
		try {
			val parts = inputString.split(",");
			val id = if (idColumn >= 0) parts(idColumn).toLong else -1;
			val weight = if (weightColumn >= 0) parts(weightColumn).toDouble else 1.0;
			val p = degrees2Point(parts(raColumn).toDouble, parts(decColumn).toDouble);

			cells += Tuple2(id, (p.phi, p.theta, weight));
		} catch {
			case e: Throwable => //System.out.println("exception: " + inputString + ", " + e.getMessage());
		}
		return cells;
	}

	def borderize(toBorderize: Iterator[(Long, (Double, Double, Double))], lowLevel: Int, highLevel: Int, maxRadius: Double): Iterator[(Long, (Long, (Double, Double, Double)))] = {
		return toBorderize.flatMap(idPoint => ObjectTools.neighboursRing(lowLevel, new Pointing(idPoint._2._2, idPoint._2._1), maxRadius, 32)
			.map(pix => Tuple2(pix, idPoint)).iterator);
		//Array(Tuple2(0L, idPoint)).toIterator);
	};

	def correlate(pixCat1Cat2: (Long, (Iterable[(Long, Double, Double)], Iterable[(Long, Double, Double)])), lowLevel: Int, highLevel: Int, binner: Binner): Array[Long] = {
		val highPix = 5; //highLevel;
		val sums = Array.fill(binner.binCount) { 0L };
		try {

			val iterCat1 = pixCat1Cat2._2._1;
			val iterCat2 = pixCat1Cat2._2._2;

			val cat2 = new ArrayBuffer[Array[Double]]();
			for (e <- iterCat2) {
				val p = new Pointing(e._3, e._2);
				if (HealpixProc.ang2pixRing(lowLevel, p) == pixCat1Cat2._1) {
					val v = new Vec3(p);
					cat2 += Array(v.x, v.y, v.z);
				}
			}

			val cat1mut = new ArrayBuffer[Array[Double]];
			for (e <- iterCat1) {
				val p = new Pointing(e._3, e._2);
				val v = new Vec3(p);
				cat1mut += Array(v.x, v.y, v.z);
			}

			val cat1 = new CountTree(cat1mut.toArray, Array.fill(cat1mut.length) { 1 })

			val xyzBins = binner.bins.map(x => 2 * Math.sin(x * 0.5)).map(x => x * x);
			val maxRadius = 2 * math.sin(binner.maxRadius * 0.5)
			//println(maxRadiusSquare, math.sqrt(maxRadiusSquare))
			for (e <- cat2) {
				cat1.countRanges(e, xyzBins, sums, maxRadius);
			}
		} catch {
			case e: Throwable => println(e.getCause + "," + e.getMessage);
		}
		//println(sums.mkString(","));
		return sums;
	}

	def increaseByDist(distSquared: Double, weight1: Double, weight2: Double, radiusSquares: Array[Double], counts: Array[Double]): Unit = {
		for (i <- (radiusSquares.length - 1) until 0 by -1) {
			if (distSquared >= radiusSquares(i - 1)) {
				//if (distSquared < radiusSquares(i)) {
				counts(i - 1) += weight1 * weight2;
				//}
				return ;
			}
		}
	}

	def correlate1(pixCat1Cat2: (Long, (Iterable[(Long, (Double, Double, Double))], Iterable[(Long, (Double, Double, Double))])), lowLevel: Int, highLevel: Int, binner: Binner): Array[Double] = {
		val highPix = 5; //highLevel;
		val sums = Array.fill(binner.binCount) { 0.0 };
		try {

			val iterCat1 = pixCat1Cat2._2._1;
			val iterCat2 = pixCat1Cat2._2._2;

			val cat2 = new ArrayBuffer[(Vec3, Pointing, Double)]();
			for (e <- iterCat2) {
				val p = new Pointing(e._2._2, e._2._1);
				if (HealpixProc.ang2pixRing(lowLevel, p) == pixCat1Cat2._1) {
					val v = new Vec3(p);
					cat2 += Tuple3(v, p, e._2._3);
				}
			}

			val arr1 = Array.fill(HealpixBase.order2Npix(highPix).toInt) { new ArrayBuffer[(Vec3, Double)] }

			for (e <- iterCat1) {
				val p = new Pointing(e._2._2, e._2._1);
				val v = new Vec3(p);
				arr1(HealpixProc.ang2pixRing(highPix, p).toInt) += Tuple2(v, e._2._3);
			}

			val xyzBins = binner.bins.map(x => 2 * Math.sin(x * 0.5)).map(x => x * x);

			for (e <- cat2) {
				val neighbours = ObjectTools.neighboursRing(highPix, e._2, binner.maxRadius, 4);
				for (neighbour <- neighbours) {
					for (p <- arr1(neighbour.toInt)) {
						val distance = e._1.sub(p._1).lengthSquared();
						if (distance < xyzBins.last) {
							ObjectTools.increaseByDist(distance, e._3, p._2, xyzBins, sums);
						}
					}
				}
			}
		} catch {
			case e: Throwable => println(e.getCause + "," + e.getMessage);
		}
		//println(sums.mkString(","));
		return sums;
	}

	def maskFilteredCat(cellCat: RDD[(Long, (Double, Double, Double))], mask: RangeSet, highLevel: Int, exclude: Boolean = false): RDD[(Long, (Double, Double, Double))] = {
		return if (mask != null) cellCat.filter(cellPair => exclude != mask.contains(HealpixProc.ang2pixNest(highLevel, new Pointing(cellPair._2._2, cellPair._2._1)))) else cellCat;
	}
}

object CellTools {
	def pixToTuple3(pix: Long, level: Int): (Long, Double, Double) = {
		val p = HealpixProc.pix2angRing(level, pix);
		return (pix, p.phi, p.theta);
	}

	def pixToLowerLevelRing(pix: Long, currentLevel: Int, lowerLevel: Int): Long = {
		return HealpixProc.ang2pixRing(lowerLevel, HealpixProc.pix2angRing(currentLevel, pix));
	}

	def pixToLowerLevelNest(pix: Long, currentLevel: Int, lowerLevel: Int): Long = {
		return pix >> 2 * (currentLevel - lowerLevel);
	}

	def parseMask(inputStrings: Iterator[String], maskLevel: Int, highLevel: Int): Iterator[RangeSet] = {

		val maskPart = new RangeSet;

		for (inputString <- inputStrings) {
			try {
				val parts = inputString.split(",");
				val pix = parts(0).toLong;

				if (maskLevel >= highLevel) {
					maskPart.add(CellTools.pixToLowerLevelNest(pix, maskLevel, highLevel));
				} else {
					maskPart.add(pix << 2 * (highLevel - maskLevel), (pix + 1) << 2 * (highLevel - maskLevel));
				}
			} catch {
				case e: Throwable => System.out.println("exception: " + inputString + ", " + e.getMessage());
			}
		}
		return List(maskPart).iterator;
	}

	def generateCatalog(bigCell: Long, size: Long, lowLevel: Int, highLevel: Int, mask: RangeSet = null): TraversableOnce[(Long, Double)] = {
		val cells = new ArrayBuffer[(Long, Double)];
		val shift = 2 * (highLevel - lowLevel);

		if (mask != null) {
			var maskCopy = new RangeSet();

			maskCopy.add(bigCell << shift, (bigCell + 1) << shift);

			maskCopy = mask.intersection(maskCopy);
			//println(bigCell, maskCopy.nval());
			Random.setSeed((bigCell + 1) * System.currentTimeMillis());
			val poisson = new PoissonSampler(size.toDouble / mask.nval(), true);
			var x = 0;
			val iter = maskCopy.valueIterator();
			while (iter.hasNext()) {
				val objectCount = poisson.sample();
				val pix = iter.next();
				if (objectCount != 0) {
					val smallCell = HealpixProc.nest2ring(highLevel, pix);
					cells += Tuple2(smallCell, objectCount);
					x += objectCount;
				}
			}
			//println(x, mask.nval());
		} else {
			val highPixFirst = bigCell << shift;
			val highPixInLow = 1L << shift;
			val poisson = new PoissonSampler(size.toDouble / HealpixBase.order2Npix(highLevel), true);
			for (i <- 0L until highPixInLow) {
				val objectCount = poisson.sample();
				if (objectCount != 0) {
					val smallCell = highPixFirst + i;
					cells += Tuple2(smallCell, objectCount);
				}
			}
		}
		return cells;
	};

	def borderize(toBorderize: Iterator[(Long, Double)], lowLevel: Int, highLevel: Int, maxRadius: Double): Iterator[(Long, (Long, Double))] = {
		return toBorderize.flatMap(cell => ObjectTools.neighboursRing(lowLevel, HealpixProc.pix2angRing(highLevel, cell._1), maxRadius, 32)
			.map(pix => Tuple2(pix, cell)).iterator);
	};

	def correlate(pixCat1Cat2: (Long, (Iterable[(Long, Double)], Iterable[(Long, Double)])), lowLevel: Int, highLevel: Int, binner: Binner): Array[Double] = {
		val sums = Array.fill(binner.binCount) { 0.0 };

		val cat2 = new LongMap[Double](_ => 0.0);

		val sum = 0.toLong;
		for (pixCountPair <- pixCat1Cat2._2._2 if (CellTools.pixToLowerLevelRing(pixCountPair._1, highLevel, lowLevel) == pixCat1Cat2._1)) {
			cat2(pixCountPair._1) += pixCountPair._2;
		}

		if (cat2.isEmpty) {
			return sums;
		}

		val beginTime = System.currentTimeMillis();
		var innerTime = 0L;

		val cat1 = Array.fill(HealpixBase.order2Npix(highLevel).toInt) { 0.0 };
		for (x <- pixCat1Cat2._2._1) { cat1(x._1.toInt) += x._2 };
		for (e <- cat2) {
			val ptg = HealpixProc.pix2angRing(highLevel, e._1);
			val v1 = e._2;

			var queries: Array[RangeSet] = HealpixProc.queryRingsRing(highLevel, e._1, ptg, binner.bins);
			//var query = HealpixProc.queryDiscRing(highLevel, ptg, binner.maxRadius);
			if (queries != null) {
				for (i <- binner.binCount - 1 to 0 by -1) {
					//val smallerQuery = HealpixProc.queryDiscRing(highLevel, ptg, binner.bins(i));
					//val ringQ = query.difference(smallerQuery);
					//sums(i) -= v1 * ringQ.convolution(cat1);
					val ringQ1 = queries(i);
					sums(i) += v1 * ringQ1.convolution(cat1);
					/*if (!ringQ.equals(ringQ1)) {
						println(i + ":\n" + ringQ.difference(ringQ1) + "\n" + ringQ1 + "\n" + ringQ + "\n");
					}
					query = smallerQuery;*/
				}
			} else {
				var query = HealpixProc.queryDiscRing(highLevel, ptg, binner.maxRadius);
				for (i <- binner.binCount - 1 to 0 by -1) {
					val smallerQuery = HealpixProc.queryDiscRing(highLevel, ptg, binner.bins(i));
					val ringQ = query.difference(smallerQuery);
					sums(i) += v1 * ringQ.convolution(cat1);
					query = smallerQuery;
				}
			}
		}
		return sums;
	}

	def maskFilteredCat(cellCat: RDD[(Long, Double)], mask: RangeSet, highLevel: Int, exclude: Boolean = false): RDD[(Long, Double)] = {
		return if (mask != null) cellCat.filter(cellPair => exclude != mask.contains(HealpixProc.ring2nest(highLevel, cellPair._1))) else cellCat;
	}

	def maskFilteredCat(cellCat: RDD[(Long, Double)], mask: RDD[(Long, Int)], highLevel: Int): RDD[(Long, Double)] = {
		return if (mask != null) cellCat.join(mask).mapValues(x => x._1)
		/*cellCat.filter(cellPair => mask.contains(HealpixProc.ring2nest(highLevel, cellPair._1)))*/ else cellCat;
	}

}

object Correlate {

	def main(argv: Array[String]) {

		val args = HashMap(
			"readdata" -> false.toString(),
			"readrand" -> false.toString(),
			"readmask" -> false.toString(),
			"datafile" -> //"file:///C://Data/Crossmatch/sdss_blue.csv",
				"file:///C://Data/Crossmatch/result_phot_short_xx_RADECw_zw02-03_zConf065.csv",
			//"file:///C://Data/Crossmatch/result_phot_short_xx_RADEC_z02-03_zConf065.csv",
			//result_phot_short_xx_RADEC_z0225-0275_zConf065.csv",
			"randfile" -> "file:///C://Data/Crossmatch/astroml_ra.csv",
			"maskfile" -> "file:///C://Data/Crossmatch/sdss_mask_9/sdss_mask_b.csv",

			"masklevel" -> 9.toString(),
			"masklowlevel" -> 3.toString(),
			"jkoutdir" -> "C://Data/Crossmatch/maskJK",
			"maskconfidence" -> 0.9.toString(),
			"datasize" -> 1000.toString(),

			"dataraidx" -> "0",
			"datadecidx" -> "1",
			"dataweightidx" -> "-1",

			/*"bins" -> 16.toString(),
			"minradius" -> (1.0 / 60.0).toString(),
			"maxradius" -> 6.toString(),
			"scale" -> "log10",*/

			"bins" -> 50.toString(),
			"minradius" -> 1.toString(),
			"maxradius" -> 15.toString(),
			"scale" -> "sqrt",

			"usecells" -> true.toString(),

			"jkcount" -> 16.toString(),
			"jackknife" -> true.toString(),

			"lowlevel" -> 0.toString(),
			"highlevel" -> 7.toString(),

			"randfactor" -> 10.toString(),
			"cores" -> 8.toString(),
			"loglevel" -> "OFF",
			"printrealfactor" -> false.toString(),
			"printprogress" -> true.toString(),
			"printbincenters" -> true.toString(),
			"printbins" -> true.toString(),
			"printnummatches" -> true.toString,
			"printinfo" -> true.toString);

		for (i <- 0 until argv.length by 2) {
			args(argv(i)) = argv(i + 1)
			if (argv(i) == "datafile") {
				args("readdata") = true.toString();
			} else if (argv(i) == "randfile") {
				args("readrand") = true.toString();
			} else if (argv(i) == "maskfile") {
				args("readmask") = true.toString();
			}
		}

		val sc = SparkContext.getOrCreate();
		sc.setLogLevel(args("loglevel"));

		var time = System.currentTimeMillis();
		def timeOnly = () => (System.currentTimeMillis() - time).toFloat / 1000;
		def timeFormatted = () => "time: " + timeOnly() + "s";

		val useCells = args("usecells").toBoolean;
		val readData = args("readdata").toBoolean;
		val readRand = args("readrand").toBoolean;
		val readMask = args("readmask").toBoolean;
		val dataRaIdx = args("dataraidx").toInt;
		val dataDecIdx = args("datadecidx").toInt;
		val dataWeightIdx = args("dataweightidx").toInt;

		val binnerLocal = Binner.create(args);

		val binner = sc.broadcast(binnerLocal);

		val binsLocal = binnerLocal.bins;
		if (args("printbins").toBoolean) {
			println("bins: " + binsLocal.map(x => math.toDegrees(x)).mkString(","));
		}

		val lowLevel = sc.broadcast(args("lowlevel").toInt);
		val highLevel = sc.broadcast(args("highlevel").toInt);
		val maskLevel = sc.broadcast(args("masklevel").toInt);

		var jackKnifeCount = if (args("jackknife").toBoolean) args("jkcount").toInt else 0;

		val lowPixCount = HealpixBase.order2Npix(lowLevel.value).toInt;
		val highPixCount = HealpixBase.order2Npix(highLevel.value).toLong;

		var mask = if (readMask) sc.textFile(args("maskfile"), lowPixCount)
			.mapPartitions(x => CellTools.parseMask(x, maskLevel.value, highLevel.value))
			.reduce { case (r1, r2) => r1.union(r2) }
		else null;

		val maskLowLevel = args("masklowlevel").toInt;
		val maskLevelDiff = 2 * (highLevel.value - maskLowLevel)

		val maskParts = sc.parallelize(mask.toArray(), math.max(jackKnifeCount, args("cores").toInt))
			.map(pix => (pix >> maskLevelDiff, pix))
			.groupByKey()
			.filter(x => x._2.size > (args("maskconfidence").toDouble) * (1 << maskLevelDiff)).map(x => x._2.toArray.sorted).collect()

		//jackKnifeCount = maskParts.length;

		mask = RangeSet.fromArray(maskParts.flatMap(x => x).sorted)

		val RAmin = 140;
		val RAmax = 220;
		val DECmin = 5;
		val DECmax = 45;
		val sdssPoly = Array(
			ObjectTools.degrees2Point(RAmin, DECmin),
			ObjectTools.degrees2Point(RAmin, DECmax),
			ObjectTools.degrees2Point(RAmax, DECmax),
			ObjectTools.degrees2Point(RAmax, DECmin));

		//val astroMLMask = HealpixProc.queryPolygonInclusiveNest(highLevel.value, sdssPoly, 16);
		//mask = astroMLMask;

		var jkLog2 = 0;
		while (2 << (jkLog2 + 1) < jackKnifeCount) jkLog2 += 1;
		val jkLog2BigHalf = (jkLog2 + 1) / 2;
		val jkLog2SmallHalf = jkLog2 / 2;
		val jkBigHalf = (2 << jkLog2BigHalf);
		val jkSmallHalf = (2 << jkLog2SmallHalf);

		val maskSorted = mask.toArray()
			.map(pix => (pix, HealpixProc.pix2angNest(highLevel.value, pix).phi, HealpixProc.pix2angNest(highLevel.value, pix).theta))
			.sortBy(f => f._2);

		val maskParts2 = new ArrayBuffer[Array[Long]];
		for (i <- 0 until jkBigHalf) {
			maskParts2 += maskSorted.slice((maskSorted.size * i) / jkBigHalf, math.min((maskSorted.size * (i + 1)) / jkBigHalf, mask.nval().toInt)).sortBy(x => x._3).map(x => x._1);
		}

		val maskFactor = if (mask != null) highPixCount.toDouble / mask.nval() else 1.0;

		val dataLines = if (readData) sc.textFile(args("datafile"), lowPixCount) else null;
		val randLines = if (readRand) sc.textFile(args("randfile"), lowPixCount) else null;
		val partitions = if (dataLines == null) lowPixCount else dataLines.getNumPartitions;

		val randFactor = args("randfactor").toDouble;

		val bigCells = sc.parallelize(ArrayBuffer.range(0, lowPixCount), partitions);

		val generateObjects = (size: Long) => bigCells.flatMap(bigCell => //ObjectTools.generateCatalog(bigCell, size, lowLevel.value, highLevel.value));
			ObjectTools.generateCatalogAstroML(bigCell, (size * (1.0 / maskFactor)).toLong, lowLevel.value, highLevel.value));
		val generateCells = (size: Long) => if (size < highPixCount) generateObjects(size).mapPartitions(cells => ObjectTools.objectsToCells(cells, highLevel.value)).reduceByKey(_ + _) else
			bigCells.flatMap(bigCell => CellTools.generateCatalog(bigCell, size, lowLevel.value, highLevel.value, null));

		val parseObjects = (lines: RDD[String], raCol: Int, decCol: Int, weightCol: Int, idCol: Int) =>
			lines.flatMap((inputString: String) => ObjectTools.parseCatalog(inputString, highLevel.value, raCol, decCol, weightCol, idCol));
		val parseObjectsToCells = (lines: RDD[String], raCol: Int, decCol: Int, weightCol: Int, idCol: Int) =>
			parseObjects(lines, raCol, decCol, weightCol, idCol).mapPartitions(cells => ObjectTools.objectsToCells(cells, highLevel.value)).reduceByKey(_ + _);

		val summarize = (a: Array[Double], b: Array[Double]) => a.zip(b).map(x => x._1 + x._2);
		def sumAndPrint(cat: (String, RDD[Array[Double]])): (String, Array[Double]) = {
			try {
				val sumArr = cat._2.reduce(summarize);
				if (args("printnummatches").toBoolean) {
					println(cat._1 + ": " + sumArr.mkString(", ") + ", " + timeFormatted());
				}
				return (cat._1, sumArr);
			} catch {
				case e: Throwable => println(cat._1 + " error:" + "\n" + e.getMessage + "\n" + e.getStackTraceString); return (cat._1, Array.fill(binner.value.binCount) { 0L });
			}
		}

		def borderizeAndcorrelate[RDDType: ClassTag](data: RDD[RDDType], rand: RDD[RDDType],
			borderizeFunc: (Iterator[RDDType], Int, Int, Double) => Iterator[(Long, RDDType)],
			correlateFunc: ((Long, (Iterable[RDDType], Iterable[RDDType])), Int, Int, Binner) => Array[Double]): Map[String, RDD[Array[Double]]] = {
			def borderize = (toBorderize: Iterator[RDDType]) => borderizeFunc(toBorderize, lowLevel.value, highLevel.value, binner.value.maxRadius);
			def correlateCells(pixCat1Cat2: Tuple2[Long, (Iterable[RDDType], Iterable[RDDType])]): Array[Double] = correlateFunc(pixCat1Cat2, lowLevel.value, highLevel.value, binner.value);
			val dataCat = data.mapPartitions(borderize).groupByKey(partitions);
			val randCat = rand.mapPartitions(borderize).groupByKey(partitions);

			return HashMap(
				"dd" -> dataCat.join(dataCat),
				"dr" -> dataCat.join(randCat),
				"rr" -> randCat.join(randCat)).par.mapValues(cat => cat.map(correlateCells)).seq;
		}

		val dataSize = sc.broadcast(args("datasize").toLong);

		//val maskRDD = if (mask == null) null else sc.parallelize(maskArray, partitions).map(x => HealpixProc.nest2ring(highLevel.value, x)).map(x => (x, 0)).cache();

		val cellData = CellTools.maskFilteredCat(if (readData) parseObjectsToCells(dataLines, dataRaIdx, dataDecIdx, dataWeightIdx, -1)
		else generateCells(dataSize.value), mask, highLevel.value).cache();
		val objectData = ObjectTools.maskFilteredCat(if (readData) parseObjects(dataLines, dataRaIdx, dataDecIdx, dataWeightIdx, -1)
		else generateObjects(dataSize.value), mask, highLevel.value).cache();

		val dSize = if (useCells) {
			cellData.values.reduce(_ + _)
		} else {
			objectData.values.map(x => x._3).reduce(_ + _)
		}
		println(dSize);
		val rSize = if (args("readrand").toBoolean) (randLines.count() * maskFactor).toLong else (dSize * randFactor * maskFactor).toLong;
		//randFactor = (rSize.toDouble / dSize).toInt;
		//println(rSize)
		val randSize = sc.broadcast(rSize);

		val cellRand = CellTools.maskFilteredCat(if (readRand) parseObjectsToCells(randLines, 0, 1, -1, -1) else generateCells(randSize.value), mask, highLevel.value).cache();
		val objectRand = ObjectTools.maskFilteredCat(if (readRand) parseObjects(randLines, 0, 1, -1, -1) else
			generateObjects(randSize.value), mask, highLevel.value).cache();

		val sums: Array[Map[String, Array[Double]]] = Array.fill(jackKnifeCount + 1) { null };

		val correlation = Array.ofDim[Double](jackKnifeCount + 1, binner.value.binCount);

		val maskPartSize = if (mask != null) (mask.nval() / math.max(jackKnifeCount, 1)).toInt else 1;

		var progress = 0;
		val pc = Vector.range(0, math.max(jackKnifeCount, 1)).par;
		pc.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(math.max(jackKnifeCount, 1)))

		var writer: PrintWriter = null;
		if (args.contains("resultfile")) {
			try {
				args("resultfile") = args("resultfile") + "_highlevel_" + highLevel.value + "_" + "_masklowlevel_" + maskLowLevel +
					"_conf_" + args("maskconfidence") + "_jk_" + jackKnifeCount + ".csv"
				writer = new PrintWriter(args("resultfile"), "UTF-8");
			} catch {
				case e: Throwable => println("error writing to file, will use stdout"); args.remove("resultfile");
			}
		}

		if (args("printbincenters").toBoolean) {
			print("bincenters: ");
			for (i <- 1 until binsLocal.length) {

				if (args.contains("resultfile")) {
					writer.print(math.toDegrees((binsLocal(i) + binsLocal(i - 1)) * 0.5) + (if (i < binsLocal.length - 1) "," else "\n"));
				}
				print(math.toDegrees((binsLocal(i) + binsLocal(i - 1)) * 0.5) + (if (i < binsLocal.length - 1) "," else "\n"));
			}
		}

		val maskColorJK = sc.parallelize(Vector.range(0, math.max(jackKnifeCount, 1)), math.max(jackKnifeCount, 1))

		val path = Path(args("jkoutdir"));
		Try(path.deleteRecursively())

		var j = 0;
		sc.parallelize(maskParts.flatMap(m => {
			j += 1; m.map(pix => (360 - math.toDegrees(HealpixProc.pix2angNest(highLevel.value, pix).phi)
				+ "," + (90 - math.toDegrees(HealpixProc.pix2angNest(highLevel.value, pix).theta)) + "," + j))
		}), 1).saveAsTextFile("file:///" + path);

		/*maskColorJK.flatMap(i => {
			val maskCol = maskParts2(i / jkSmallHalf);
			val m = maskCol.slice((i % jkSmallHalf) * maskCol.length / jkSmallHalf, ((i % jkSmallHalf) + 1) * maskCol.length / jkSmallHalf)
			m.map(pix => (360 - math.toDegrees(HealpixProc.pix2angNest(highLevel.value, pix).phi) + "," + (90 - math.toDegrees(HealpixProc.pix2angNest(highLevel.value, pix).theta)) + "," + i))
		}).coalesce(1).saveAsTextFile("file:///" + path);*/

		/*val maskArray = mask.toArray();
		maskColorJK.flatMap(i => {
			val m = maskArray.slice(i * maskPartSize, (i + 1) * maskPartSize)
			m.map(pix => (360 - math.toDegrees(HealpixProc.pix2angNest(highLevel.value, pix).phi) + "," + (90 - math.toDegrees(HealpixProc.pix2angNest(highLevel.value, pix).theta)) + "," + i))
		}).coalesce(1).saveAsTextFile("file:///" + path);*/

		println(jackKnifeCount);

		pc.foreach(i => {
			val maskCol = maskParts2(i / jkSmallHalf);
			val m = if (mask == null) null else
				RangeSet.fromArray(maskParts(i))
			//RangeSet.fromArray(maskCol.slice((i % jkSmallHalf) * maskCol.length / jkSmallHalf, ((i % jkSmallHalf) + 1) * maskCol.length / jkSmallHalf).sorted)
			//RangeSet.fromArray(maskArray.slice(i * maskPartSize, (i + 1) * maskPartSize))
			//mask.difference(RangeSet.fromArray(maskParts(i)))
			//sc.parallelize(
			//mask.difference(RangeSet.fromArray(maskArray.slice(i * maskPartSize, (i + 1) * maskPartSize))).toArray(), partitions)
			//.map(x => HealpixProc.nest2ring(highLevel.value, x)).map(x => (x, 0));

			var nrm = randFactor;
			val result = if (useCells) {
				val maskFilteredData = CellTools.maskFilteredCat(cellData, m, highLevel.value, true).cache();
				val maskFilteredRand = CellTools.maskFilteredCat(cellRand, m, highLevel.value, true).cache();
				//nrm = (rSize * m.nval()).toDouble / (maskFilteredData.values.reduce(_ + _) * highPixCount);

				nrm = maskFilteredRand.values.reduce(_ + _) / maskFilteredData.values.reduce(_ + _)
				println(maskFilteredRand.values.reduce(_ + _))

				/*nrm = (rSize * math.max(jackKnifeCount - 1, 1).toDouble /
					(maskFilteredData.values.reduce(_ + _) * maskFactor * math.max(jackKnifeCount, 1)));*/

				borderizeAndcorrelate(maskFilteredData, maskFilteredRand, CellTools.borderize, CellTools.correlate);
			} else {
				/*val maskFilteredData = objectData.sample(withReplacement = true, fraction = 1.0).cache()
				val maskFilteredRand = objectRand;*/

				val maskFilteredData = ObjectTools.maskFilteredCat(objectData, m, highLevel.value, true).cache();
				val maskFilteredRand = ObjectTools.maskFilteredCat(objectRand, m, highLevel.value, true);

				/*val path = Path("C://Data/Crossmatch/randcat/" + i);
				Try(path.deleteRecursively())
				maskFilteredRand.map(t => (360 - math.toDegrees(t._2._1)) + "," + (90 - math.toDegrees(t._2._2))).coalesce(1).saveAsTextFile("file:///" + path);*/

				/*val path2 = Path("C://Data/Crossmatch/maskedcat/" + i);
				Try(path2.deleteRecursively())
				maskFilteredData.map(t => (360 - math.toDegrees(t._2._1)) + "," + (90 - math.toDegrees(t._2._2))).coalesce(1).saveAsTextFile("file:///" + path2);*/

				nrm = maskFilteredRand.map(x => x._2._3).reduce(_ + _) / maskFilteredData.map(x => x._2._3).reduce(_ + _)

				borderizeAndcorrelate(maskFilteredData, maskFilteredRand, ObjectTools.borderize, ObjectTools.correlate1)
			};

			sums(i) = result.map(cat => sumAndPrint(cat));

			if (args("printrealfactor").toBoolean) println(nrm);
			correlation(i) = Array.fill(binnerLocal.binCount) { 0.0 };
			for (bin <- 0 until binnerLocal.binCount) {
				correlation(i)(bin) = (nrm * nrm * sums(i)("dd")(bin) - 2 * nrm * sums(i)("dr")(bin) + sums(i)("rr")(bin)).toDouble / sums(i)("rr")(bin);
			}

			if (args("jackknife").toBoolean && args("printprogress").toBoolean) {
				progress += 1;
				println("jackknife progress: " + progress + " of " + (jackKnifeCount) + ", " + timeFormatted())
			}
		});

		if (!args("jackknife").toBoolean) {
			if (args.contains("resultfile")) {
				writer.println(correlation.last.mkString(","))
			}
			println("\n" + correlation.last.mkString(","));
		} else {

			for (j <- 0 until binnerLocal.binCount) {
				correlation.last(j) = 0.0;
				for (i <- 0 until jackKnifeCount) {
					correlation.last(j) += correlation(i)(j)
				}
				correlation.last(j) /= (jackKnifeCount);
			}

			if (args.contains("resultfile")) {
				writer.println(correlation.last.mkString(","))
			}
			println("\n" + correlation.last.mkString(","));

			val covariation = Array.ofDim[Double](binner.value.binCount, binner.value.binCount);
			for (i <- 0 until binnerLocal.binCount) {
				for (j <- 0 until binnerLocal.binCount) {
					for (k <- 0 until jackKnifeCount) {
						covariation(i)(j) += (correlation.last(i) - correlation(k)(i)) * (correlation.last(j) - correlation(k)(j));
					}
					covariation(i)(j) *= (jackKnifeCount - 1).toDouble / jackKnifeCount;
				}
			}

			val errCorr = Array.ofDim[Double](binner.value.binCount, binner.value.binCount);
			for (i <- 0 until binnerLocal.binCount) {
				for (j <- 0 until binnerLocal.binCount) {
					errCorr(i)(j) = covariation(i)(j) / math.sqrt(covariation(i)(i) * covariation(j)(j));
				}
			}

			var errorbars = Array.ofDim[Double](binner.value.binCount);

			for (i <- 0 until binnerLocal.binCount) {
				errorbars(i) = math.sqrt(covariation(i)(i));
			}

			if (args.contains("resultfile")) {
				writer.println(errorbars.mkString(",") + "\n")

				writer.println("\n" + covariation.map(c => c.mkString(",")).mkString("\n"))

				writer.println("\n" + errCorr.map(c => c.mkString(",")).mkString("\n"))

				writer.println("\n" + correlation.map(c => c.mkString(",")).mkString("\n"))
			}
			println(errorbars.mkString(",") + "\n")

			println("\n" + covariation.map(c => c.mkString(",")).mkString("\n"))

			println("\n" + errCorr.map(c => c.mkString(",")).mkString("\n"))

			println("\n" + correlation.map(c => c.mkString(",")).mkString("\n"))

		}

		if (args("printinfo").toBoolean && args.contains("resultfile")) {
			writer.println("\ndsize,rsize,lowlevel,highlevel,usecells,time")
			writer.println(dSize.toLong + "," + (dSize * randFactor).toLong + "," + lowLevel.value + "," + highLevel.value + "," + useCells + "," + timeOnly());
		}
		if (args("printinfo").toBoolean) {
			println("\ndsize,rsize,lowlevel,highlevel,usecells,scale,time")
			println(dSize.toLong + "," + (dSize * randFactor).toLong + "," + lowLevel.value + "," + highLevel.value + "," + useCells + "," + args("scale") + "," + timeOnly());
		}

		if (args.contains("resultfile")) {
			println("\nresult written to " + args("resultfile"))
			writer.close()
		}
	}
}

object Local {
	def main(args: Array[String]) {

		val newArgs = new HashMap[String, String];
		var cores = 1;

		for (i <- 0 until args.length by 2 if (args(i) == "cores")) {
			cores = args(i + 1).toInt;
		}

		val conf = new SparkConf()
			.setAppName("Scala Correlate")
			.setMaster("local[" + cores + "]")
			.set("spark.driver.maxResultSize", "2g")
			.set("spark.executor.memoryOverhead", "200m")
			.set("spark.executor.memory", "3g")
			.set("spark.worker.memory", "24g")
			.set("spark.driver.memory", "2g")
			.set("spark.sql.warehouse.dir", "file:///c:/Spark/spark-warehouse");

		val sc = new SparkContext(conf);
		sc.hadoopConfiguration.setLong("fs.local.block.size", 256 * 1024 * 1024);
		Correlate.main(args);
		sc.stop();
	}
}

object SubmitLocal {
	def main(args: Array[String]) {

		val newArgs = new HashMap[String, String];

		var cores = 8;

		for (i <- 0 until args.length by 2 if (args(i) == "cores")) {
			cores = args(i + 1).toInt;
		}

		val conf = new SparkConf()
			.setAppName("Scala Correlate")
			.setMaster("local[" + cores + "]")

		val sc = SparkContext.getOrCreate(conf);
		sc.hadoopConfiguration.setLong("fs.local.block.size", 256 * 1024 * 1024);
		Correlate.main(args);
		sc.stop();
	}
}