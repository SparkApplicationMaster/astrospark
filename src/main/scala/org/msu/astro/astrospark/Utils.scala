package org.msu.astro.astrospark

import healpix.essentials._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Utils {
  def readDF(path: String, format: String, schema: StructType, needed: Boolean = true, log: Logger = LogManager.getLogger("Refactored")): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    try {
      if (needed) {
        val nonEmptyDF = spark.read
          .option("mode", "DROPMALFORMED")
          .option("header", "true")
          .option("inferSchema", value = false)
          .schema(schema)
          .format(format)
          .load(path)
        log.warn("Successfully read dataframe [" + path + "]")
        return nonEmptyDF
      }
    } catch { case e: Throwable => log.fatal("Cannot read dataframe [" + path + "]: ", e) }
    return emptyDF
  }

  def neighboursRing(lowlevel: Int, point: Pointing, radius: Double, factor: Int) =
    HealpixProc.queryDiscInclusiveRing(lowlevel, point, radius, factor).toArray

  val raToRadians = radians(lit(360.0) - col("ra")) as "ra"
  val decToRadians = radians(lit(90.0) - col("dec")) as "dec"

  def pixFromRaDec(level: Int, scheme: String = "RING", convertToRadians: Boolean = false) =
    udf((ra: Double, dec: Double) =>
      if (scheme == "RING")
        HealpixProc.ang2pixRing(level, new Pointing(dec, ra))
      else
        HealpixProc.ang2pixNest(level, new Pointing(dec, ra)),
      LongType)(
      if (convertToRadians) raToRadians else col("ra"),
      if (convertToRadians) decToRadians else col("dec"))

  def lowPixFromHighPix(lowlevel: Int, highlevel: Int, scheme: String = "RING") =
    udf((pix: Long) =>
      if (scheme == "RING")
        HealpixProc.ang2pixRing(lowlevel, HealpixProc.pix2angRing(highlevel, pix))
      else
        pix >>> 2 * (highlevel - lowlevel)
    , LongType)

  def highPixBorderize(args: Args) = udf((highpix: Long) => Utils.neighboursRing(args.lowlevel,
    HealpixProc.pix2angRing(args.highlevel, highpix), args.maxradius, args.ringfactor), ArrayType(LongType))(col("highpix"))

  def raDecBorderize(args: Args) = udf((ra: Double, dec: Double) => Utils.neighboursRing(args.lowlevel,
    new Pointing(dec, ra), args.maxradius, args.ringfactor), ArrayType(LongType))(col("ra"), col("dec"))

  def haversine = {
    val lat1 =  lit(math.Pi / 2) - col("a.dec")
    val lat2 = lit(math.Pi / 2) - col("b.dec")
    val dLat = lat1 - lat2
    val dLon = col("a.ra") - col("b.ra")
    lit(2) * asin(sqrt(sin(dLat / 2) * sin(dLat / 2) + sin(dLon / 2) * sin(dLon / 2) * cos(lat1) * cos(lat2)))
  }

  def euclid = {
    sqrt(
      Seq("x", "y", "z")
        .map(coord => col("a." + coord) - col("b." + coord))
        .map(c => c * c)
        .reduce(_ + _)
    )
  }

  implicit class PointingUtils(val p: Pointing) {
    def toArray = Array(p.phi, p.theta)

    def haversine(p2: Pointing) = {
      val lat1 =  math.Pi / 2 - p.theta
      val lat2 = math.Pi / 2 - p2.theta
      val dLat = lat1 - lat2
      val dLon = p.phi - p2.phi
      2 * math.asin(math.sqrt(math.pow(math.sin(dLat / 2), 2) + math.pow(math.sin(dLon / 2), 2) * math.cos(lat1) * math.cos(lat2)))
    }
  }

  implicit class DFUtils(val df: DataFrame) {
    def drop(c: List[String]) = c.foldLeft(df) { case (d, col) => d.drop(col) }
    def renameForJoin(): DataFrame = df.columns.foldLeft(df)((tmp, colName: String) => tmp.withColumnRenamed(colName, colName + "_right"))

    def zipWithIndex(offset: Long = 1, indexName: String = "index") = {
      val dfWithPartitionId = df.withColumn("partition_id", spark_partition_id()).withColumn("inc_id", monotonically_increasing_id())

      val partitionOffsets = dfWithPartitionId
        .groupBy("partition_id")
        .agg(count(lit(1)) as "cnt", first("inc_id") as "inc_id")
        .orderBy("partition_id")
        .select(sum("cnt").over(Window.orderBy("partition_id")) - col("cnt") - col("inc_id") + lit(offset) as "cnt" )
        .collect()
        .map(_.getLong(0))

      dfWithPartitionId
        .withColumn("partition_offset", udf((partitionId: Int) => partitionOffsets(partitionId), LongType)(col("partition_id")))
        .withColumn(indexName, col("partition_offset") + col("inc_id"))
        .drop("partition_id", "partition_offset", "inc_id")
    }
  }

  implicit class AstroDFUtils(val df: DataFrame) {

    def groupByAndPack(namedColumns: String*) = df
      .select(namedColumns.map(col) :+ (struct(df.drop(namedColumns: _*).columns.map(col): _*) as "object"): _*)
      .groupBy(namedColumns.head, namedColumns.tail: _*)

    def convertRaDecToRadians() = df.withColumn("ra", raToRadians).withColumn("dec", decToRadians)

    def toPixXYZ(pixLevel: Int) = df.select(
      col("id"),
      pixFromRaDec(pixLevel, "NESTED") as "pix",
      sin("dec") * cos("ra") as "x",
      sin("dec") * sin("ra") as "y",
      cos("dec") as "z"
    )

    def borderize(args: Args) = df.withColumn("lowpix", explode(if (args.usecells) highPixBorderize(args) else raDecBorderize(args)))

    def getWeight(args: Args) = if (!args.useweight) lit(1.0) else if (df.columns.contains("weight")) coalesce(col("weight"), lit(0.0)) else lit(0.0) as "weight"

    def pixelize(args: Args) = df.groupBy("pix").agg(sum("weight") as "weight")

    def toWeightedPixels(args: Args) = df
      .select(
        getWeight(args) as "weight",
        pixFromRaDec(args.highlevel) as "pix"
      )

      .pixelize(args)
      /*.withColumn("lowpix", lowPixFromHighPix(args.lowlevel, args.highlevel)(col("pix")))
      .withColumn("radec", udf((pix: Long) => HealpixProc.pix2angRing(args.highlevel, pix).toArray, ArrayType(DoubleType))(col("pix")))
      .select(
        col("pix"),
        col("lowpix"),
        col("weight"),
        col("radec")(0) as "ra",
        col("radec")(1) as "dec"
      )*/


    /*def readAndPrepareForCorrelation(args: Args): DataFrame = {
        df.explodeByCircles(args)

      result = if (!args.usecells) result.groupByAndPack("lowpix", "highpix").agg(collect_list("object") as "radec")
      else result.groupBy("lowpix", "highpix").agg(sum("weight") as "weight")

      result = result.groupByAndPack("lowpix").agg(collect_list("object") as "objects").cache()
      result.printSchema()
      return result
    }*/

    def explodeByRadius(args: Args): DataFrame = df
      .withColumn("lowpix", explode(
        udf((pix: Long) =>
          HealpixProc.queryDiscInclusiveNest(
            args.lowlevel,
            HealpixProc.pix2angRing(args.highlevel, pix),
            args.binner.maxRadius,
            4
          ).toArray
        , ArrayType(LongType))(col("pix"))
      ))

    def explodeByCircles(args: Args): DataFrame = df
      .withColumn("rings", udf((pix: Long) => {
        val ptg = HealpixProc.pix2angRing(args.highlevel, pix)
        var queries: Array[RangeSet] = HealpixProc.queryRingsRing(args.highlevel, ptg, args.binner.bins)
        if (queries == null) {
          queries = new Array[RangeSet](args.binner.binCount)
          var query = HealpixProc.queryDiscRing(args.highlevel, ptg, args.binner.maxRadius)
          for (i <- args.binner.binCount - 1 to 0 by -1) {
            val smallerQuery = HealpixProc.queryDiscRing(args.highlevel, ptg, args.binner.bins(i))
            queries(i) = query.difference(smallerQuery)
            query = smallerQuery
          }
        }
        queries.map(_.toArray)
      }, ArrayType(ArrayType(LongType)))(col("pix")))
      .select(df.columns.map(col) :+ (posexplode(col("rings")) as Seq("ring_index", "pix_right")):_*).withColumn("pix_right", explode(col("pix_right")))

    def compressByCircles(args: Args): DataFrame = df
      .withColumn("rings", udf((pix: Long) => {
        val ptg = HealpixProc.pix2angRing(args.highlevel, pix)
        var queries: Array[RangeSet] = HealpixProc.queryRingsRing(args.highlevel, ptg, args.binner.bins)
        if (queries == null) {
          queries = new Array[RangeSet](args.binner.binCount)
          var query = HealpixProc.queryDiscRing(args.highlevel, ptg, args.binner.maxRadius)
          for (i <- args.binner.binCount - 1 to 0 by -1) {
            val smallerQuery = HealpixProc.queryDiscRing(args.highlevel, ptg, args.binner.bins(i))
            queries(i) = query.difference(smallerQuery)
            query = smallerQuery
          }
        }
        queries
          .map {
            case (r) =>
              (0 until r.nranges())
                .map(i => Array(r.ivbegin(i), r.ivend(i)))
          }
      }, ArrayType(ArrayType(ArrayType(LongType))))(col("pix")))

    //def correlateObjects = udf(objects: ArrayType(StructType(List(DoubleType, DoubleType, DoubleType))))

    def explodeByPixNeighbors(args: Args) = df.withColumn("pix", explode(udf((pix: Long) =>
      (HealpixProc.neighboursNest(args.highlevel, pix).filterNot(_ == -1) :+ pix).map(_ >>> (args.highlevel - args.lowlevel) * 2).distinct,
      ArrayType(LongType))(col("pix"))))

    def explodeByQueryDiscRing(args: Args) = df.withColumn("pix", explode(udf((ra: Double, dec: Double) =>
      HealpixProc.queryDiscInclusiveRing(args.highlevel, new Pointing(dec, ra), args.maxradius, 4).toArray,
      ArrayType(LongType))(col("ra"), col("dec"))))
  }
}
