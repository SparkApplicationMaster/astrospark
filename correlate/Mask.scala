package correlate

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import healpix.essentials.HealpixProc
import org.apache.spark.SparkConf
import org.apache.spark.RangePartitioner
import scala.reflect.io.File

object Mask {
	def main(args: Array[String]) {
		if (args.length < 3) {
			println("usage: createmask <data_file> <healpix_level> <mask_file>")
			return ;
		}
		val conf = new SparkConf()
			.setAppName("Calibration")
			.setMaster("local[*]")
			.set("spark.driver.maxResultSize", "4g")
			.set("spark.memory.fraction", "0.9")
			.set("spark.executor.memoryOverhead", "300m")
			.set("spark.memory.storageFraction", "0.9")
			.set("spark.executor.memory", "2g")
			.set("spark.worker.memory", "16g")
			.set("spark.driver.memory", "4g")
		/*.set("spark.sql.warehouse.dir", "file:///c:/Spark/spark-warehouse");*/

		val sc = SparkContext.getOrCreate(conf)
		sc.hadoopConfiguration.setLong("fs.local.block.size", 128 * 1024 * 1024);

		def parse(inputString: String): TraversableOnce[(Long, (Double, Double, Long))] = {
			val cells = new ArrayBuffer[(Long, (Double, Double, Long))];
			try {
				val parts = inputString.split(",");
				val p = ObjectTools.degrees2Point(parts(0).toDouble, parts(1).toDouble);
				val cell = HealpixProc.ang2pixRing(args(1).toInt, p);
				cells += Tuple2(cell, (parts(2).toDouble, parts(3).toDouble, 1L));
			} catch {
				case e: Throwable => //System.out.println("exception: " + inputString + ", " + e.getMessage());
			}
			return cells;
		}

		def pixToRaDecString(pix: Long): String = {
			val p = HealpixProc.pix2angRing(args(1).toInt, pix);
			return (360 - math.toDegrees(p.phi)) + "," + (90 - math.toDegrees(p.theta))
		}

		val mask = sc.textFile(args(0), 8)
			.flatMap(parse)
			.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3));

		val filtered = mask.filter(t => {
			//t._2._1 / t._2._3 < 0.2 && t._2._3 > 15;
			//t._2._1 / t._2._3 < 0.2 && t._2._2 / t._2._3 > 25 && t._2._3 > 15;
			t._2._1 / t._2._3 < 0.1 && t._2._2 / t._2._3 > 25 && t._2._3 > 15;
		});

		val size = filtered.count();

		val result = filtered.sortByKey().zipWithIndex().map(p => (p._1, p._2 / (size / 32)))
			.map { case (t, i) => t._1 + "," + pixToRaDecString(t._1) + "," + (t._2._1 / t._2._3) + "," + (t._2._2 / t._2._3) + "," + t._2._3 + "," + i };

		result.coalesce(1).saveAsTextFile(args(2))

	}
}