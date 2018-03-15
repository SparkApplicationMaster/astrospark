package org.msu.astro.astrospark

import healpix.essentials.HealpixProc
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

import scala.collection.mutable

object AstroSpark {
  import Utils._

  @transient lazy val log: Logger = LogManager.getLogger("Refactored")

  def correlate(args: Args): Unit = {
    if (args.printbins) {
      println("bins: " + args.binner.bins.map(x => math.toDegrees(x)).mkString(","))
    }

    val read = Utils.readDF(args.datafile, args.dataformat, args.dataschema, args.readdata, log).convertRaDecToRadians().cache()

    println(read.count())

    if (read.count() == 0) {
      return
    }

    val data = /*Utils.readDF(args.datafile, args.dataformat, args.dataschema, args.readdata, log).convertRaDecToRadians().*/read.toWeightedPixels(args).cache()
    //val rand = Utils.readDF(args.randfile, args.randformat, args.randschema, args.readrand, log).toWeightedPixels(args)
    val rand = read.zipWithIndex().as("a").join(read.orderBy(functions.rand()).zipWithIndex().as("b"), "index").select("a.ra", "b.dec").toWeightedPixels(args).cache()

    println(data.count())

    val dd = data.explodeByRadius(args).as("a")
      .join(data.as("b"), col("b.lowpix") === col("a.lowpix"))
      .withColumn("dist", haversine)
      .where(col("dist").between(args.binner.minRadius, args.binner.maxRadius))
      .withColumn("ring_index", udf((dist: Double) => args.binner.distToIndex(dist), IntegerType)(col("dist")))
      .groupBy("ring_index")
      .agg(sum(col("a.weight") * col("b.weight")) as "sum")
      //.orderBy("ring_index")
      //.cache()
      .as("dd")

    val dr = data.explodeByRadius(args).as("a")
      .join(rand.as("b"), col("b.lowpix") === col("a.lowpix"))
      .withColumn("dist", haversine)
      .where(col("dist").between(args.binner.minRadius, args.binner.maxRadius))
      .withColumn("ring_index", udf((dist: Double) => args.binner.distToIndex(dist), IntegerType)(col("dist")))
      .groupBy("ring_index")
      .agg(sum(col("a.weight") * col("b.weight")) as "sum")
      //.orderBy("ring_index")
      //.cache()
      .as("dr")

    val rr = rand.explodeByRadius(args).as("a")
      .join(rand.as("b"), col("b.lowpix") === col("a.lowpix"))
      .withColumn("dist", haversine)
      .where(col("dist").between(args.binner.minRadius, args.binner.maxRadius))
      .withColumn("ring_index", udf((dist: Double) => args.binner.distToIndex(dist), IntegerType)(col("dist")))
      .groupBy("ring_index")
      .agg(sum(col("a.weight") * col("b.weight")) as "sum")
      //.orderBy("ring_index")
      //.cache()
      .as("rr")

    dd.join(dr, "ring_index")
      .join(rr, "ring_index")
      .select(
        col("ring_index"),
        col("dd.sum") as "dd",
        col("dr.sum") as "dr",
        col("rr.sum") as "rr")
      .withColumn("corr", (col("dd") - col("dr") * 2 + col("rr")) / col("rr"))
      .orderBy("ring_index").show(100)

    //data.compressByCircles(args).select(size(col("rings")) as "size").agg(sum("size")).show()//.write.mode("overwrite").save("file:///C://Data/Crossmatch/out_2.csv")


    /*data.explodeByCircles(args).as("a")
      .join(data.as("b"), col("b.pix") === col("a.pix_right"))
      .withColumn("dist", haversine)
      .groupBy("a.ring_index").agg(sum(col("a.weight") * col("b.weight")) as "sum")
      .withColumn("radius", udf((x: Int) => ((args.binner.bins(x) + args.binner.bins(x + 1)) * 0.5).toDegrees, DoubleType)(col("a.ring_index")))
      .orderBy("a.ring_index")/*.coalesce(1)*/
      .show(100)*/
    //.write.format("csv").mode("overwrite").option("header", "true").save("file:///C://Data/Crossmatch/out_1.csv")

    //data.alias("d").join(rand.alias("r"), "lowpix").withColumn("sums", )

    //val bigCells = args.session.sparkContext.parallelize(List.range(0L, args.lowpixcount), args.partitions)
  }

  def crossmatch(args: Args): Unit = {




    val data = Utils.readDF(args.datafile, args.dataformat, args.dataschema, args.readdata, log)/*.repartition(300).write.parquet("file:///C://Data/Crossmatch/galex_300")*/.convertRaDecToRadians().withColumn("pix", pixFromRaDec(args.highlevel, "NESTED"))
    val data2 = Utils.readDF(args.datafile2, args.dataformat2, args.dataschema2, args.readdata2, log)/*.repartition(300).write.parquet("file:///C://Data/Crossmatch/sdss_300")*/.convertRaDecToRadians().withColumn("pix", pixFromRaDec(args.lowlevel, "NESTED"))

    val w = Window.partitionBy("id").orderBy("dist").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val w2 = Window.partitionBy("id_2").orderBy("dist").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)


    /*data2.where(col("pix").isin(
      1224604685
      ,1224604687
      ,1224604698
      ,1224604699
      ,1224604697
      ,1224604691
      ,1224604690
      ,1224604679
      ,1224604696)
    ).show(100)*/

    data
      //.explodeByQueryDiscRing(args)
      .explodeByPixNeighbors(args)/*.where(col("id") === 6380274870404516284L)*/
      .as("a")
      .join(
        data2
          //.where(col("id") === 1237646646201745656L)
          .as("b"),
        col("a.pix") === col("b.pix"))
      .withColumn("dist", degrees(haversine))
      .filter(col("dist") < args.maxradiusdeg)
      .select(
        col("a.id") as "id",
        col("b.id") as "id_2",
        col("a.ra") as "ra",
        col("a.dec") as "dec",
        col("b.ra") as "ra_2",
        col("b.dec") as "dec_2",
        col("dist"))
      .select(
        col("id") as "id",
        first("id_2") over w as "id_2",
        first("dist") over w as "dist",
        col("ra") as "ra",
        col("dec") as "dec",
        first("ra_2") over w as "ra_2",
        first("dec_2") over w as "dec_2")
      .distinct()
      .select(
        first("id") over w2 as "id",
        col("id_2") as "id_2",
        first("dist") over w2 as "dist",
        first("ra") over w2 as "ra",
        first("dec") over w2 as "dec",
        col("ra_2") as "ra_2",
        col("dec_2") as "dec_2")
      .distinct()
      .withColumn("dist", col("dist") * 3600)
      //.count()
      //.show(20)
      //.coalesce(1)
      .write.mode("overwrite")/*.option("header", true).csv(args.resultdir)*/.parquet(args.resultdir)
  }

  def crossmatchXYZ(args: Args) = {
    val data = Utils.readDF(args.datafile, args.dataformat, args.dataschema, args.readdata, log).convertRaDecToRadians().toPixXYZ(args.highlevel)
    val data2 = Utils.readDF(args.datafile2, args.dataformat2, args.dataschema2, args.readdata2, log).convertRaDecToRadians().toPixXYZ(args.lowlevel)

    println(data.count())
    println(data2.count())

    val w = Window.partitionBy("id").orderBy("dist").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val w2 = Window.partitionBy("id_2").orderBy("dist").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val hord = 2 * Math.sin(args.maxradius * 0.5)

    //println(
    data
      //.explodeByQueryDiscRing(args)
      .explodeByPixNeighbors(args)/*.where(col("id") === 6380274870404516284L)*/
      .as("a")
      .join(
        data2
          //.where(col("id") === 1237646646201745656L)
          .as("b"),
        col("a.pix") === col("b.pix"))
      .withColumn("dist", euclid)
      //.count())
      .filter(col("dist") < hord)
      .select(
        col("a.id") as "id",
        col("b.id") as "id_2"/*,
        col("a.ra") as "ra",
        col("a.dec") as "dec",
        col("b.ra") as "ra_2",
        col("b.dec") as "dec_2"*/,
        col("dist"))
      .select(
        col("id") as "id",
        first("id_2") over w as "id_2",
        first("dist") over w as "dist"/*,
        col("ra") as "ra",
        col("dec") as "dec",
        first("ra_2") over w as "ra_2",
        first("dec_2") over w as "dec_2"*/)
      .distinct()
      .select(
        first("id") over w2 as "id",
        col("id_2") as "id_2",
        first("dist") over w2 as "dist"/*,
        first("ra") over w2 as "ra",
        first("dec") over w2 as "dec",
        col("ra_2") as "ra_2",
        col("dec_2") as "dec_2"*/)
      .distinct()
      //.withColumn("dist", col("dist") * 3600)
      //.count()
      //.show(20)
      //.coalesce(1)
      .write.mode("overwrite")/*.option("header", true).csv(args.resultdir)*/.parquet(args.resultdir)
  }

  def crossmatchGroupped(args: Args) = {
    val data = Utils.readDF(args.datafile, args.dataformat, args.dataschema, args.readdata, log)
      .convertRaDecToRadians()
      .withColumn("pix", pixFromRaDec(args.highlevel, "NESTED"))
      .explodeByPixNeighbors(args)
      .groupBy("pix")
      .agg(
        collect_list("id") as "id_list",
        collect_list("ra") as "ra_list",
        collect_list("dec") as "dec_list")

    val data2 = Utils.readDF(args.datafile2, args.dataformat2, args.dataschema2, args.readdata2, log)
      .convertRaDecToRadians()
      .withColumn("pix", pixFromRaDec(args.lowlevel, "NESTED"))
      .groupBy("pix")
      .agg(
        collect_list("id") as "id_list",
        collect_list("ra") as "ra_list",
        collect_list("dec") as "dec_list")

    data.as("a").join(data2.as("b"), "pix").
      select(
        size(col("a.id_list")),
        size(col("b.id_list")),
        size(col("a.ra_list")),
        size(col("a.dec_list")),
        size(col("b.ra_list")),
        size(col("a.dec_list"))
      ).show(20000)
  }

  def main(argv: Array[String]) {

    val args = new Args(argv)

    SparkSession.builder().getOrCreate().sparkContext.setLogLevel(args.loglevel)

    val time = System.currentTimeMillis()
    def timeOnly = () => (System.currentTimeMillis() - time).toFloat / 1000
    def timeFormatted = () => "time: " + timeOnly() + "s"

    args.algorithm match {
      case "correlate" => correlate(args)
      case "crossmatch" => crossmatchXYZ(args)
    }

    println(timeFormatted())
  }
}

object SubmitLocal {
  def main(args: Array[String]) {

    val newArgs = new mutable.HashMap[String, String]

    var cores = 8

    for (i <- args.indices by 2 if args(i) == "cores") {
      cores = args(i + 1).toInt
    }

    val conf = new SparkConf()
      .setAppName("Scala Correlate")
      .setMaster("local[" + cores + "]")
      .set("spark.default.parallelism", "100" /*+ cores*/)
      .set("spark.sql.shuffle.partitions", "300")
      .set("spark.sql.shuffle.service", "false")
      .set("spark.sql.codegen", "false")
      .set("spark.executor.memory", "3072m")
      .set("spark.memory.fraction", "0.8")
      .set("spark.memory.storageFraction", "0.1")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")

    val sc = SparkContext.getOrCreate(conf)
    sc.hadoopConfiguration.setInt("fs.local.block.size", 1024 * 1024 * 1024)

    AstroSpark.main(args)
    sc.stop()
  }
}
