package org.msu.astro.astrospark

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import healpix.essentials._

class Args(args: Map[String, String]) extends Serializable {
  //println(args.map { case (x, y) => x + ":" + y }.mkString(","))
  val algorithm: String = Args.stringOrElse(args, "algorithm", "crossmatch") //"correlate")

  val readdata: Boolean = Args.booleanOrElse(args, "readdata", args.contains("datafile") || true)
  val readrand: Boolean = Args.booleanOrElse(args, "readrand", args.contains("randfile"))
  val readmask: Boolean = Args.booleanOrElse(args, "readmask", args.contains("maskfile"))
  val usecells: Boolean = Args.booleanOrElse(args, "usecells", true)
  val jackknife: Boolean = Args.booleanOrElse(args, "jackknife", false)
  val useweight: Boolean = Args.booleanOrElse(args, "useweight", false)

  val datafile: String = Args.stringOrElse(args, "datafile",
    //"file:///C://Data/Crossmatch/sdss_blue.csv"
   // "file:///C://Data/Crossmatch/galex_q_small"
    "file:///C://Data/Crossmatch/galex_q"
    //"file:///C://Data/Crossmatch/3XMM_DR5cat_slim_v1.0_radec_short.csv"
    // "file:///C://Data/Crossmatch/result_phot_short_xx_RADECw_zw02-03_zConf065.csv"
    //"file:///D://Data/Crossmatch_C/result_phot_short_xx_RADEC_z02-03_zConf065.csv"
    //file:///C://Data//Crossmatch/result_phot_short_xx_RADEC_z0225-0275_zConf065.csv"
    //"file:///D://Data/Crossmatch/GalexGR6_radecN_mesch.csv"
  )

  val randfile: String = Args.stringOrElse(args, "randfile", "file:///C://Data/Crossmatch/astroml_random.csv")
  val maskfile: String = Args.stringOrElse(args, "maskfile", "file:///C://Data/Crossmatch/sdss_mask_9/sdss_mask_b.csv")
  val jkregionsoutdir: String = Args.stringOrElse(args, "jkregionsoutdir", "C://Data/Crossmatch/maskJK")

  val dataformat = Args.stringOrElse(args, "dataformat", "csv")
  val randformat = Args.stringOrElse(args, "randformat", "csv")

  val dataschema: StructType = Args.structTypeOrElse(args, "dataschema", StructType(List(
    StructField("ra", DoubleType, false),
    StructField("dec", DoubleType, false))))
  val randschema: StructType = Args.structTypeOrElse(args, "randschema", StructType(List(
    StructField("ra", DoubleType, false),
    StructField("dec", DoubleType, false)
    //,StructField("weight", DoubleType, false)
  )))

  val readdata2: Boolean = Args.booleanOrElse(args, "readdata2", args.contains("datafile2") || true)
  val datafile2: String = Args.stringOrElse(args, "datafile2", "file:///C://Data/Crossmatch/" +
    //"sdss_q_small"
    "sdss_q"
    //"FIRST_catalog_14dec17_radec_short.csv"
    )
  val dataformat2 = Args.stringOrElse(args, "dataformat2", "csv")
  val dataschema2: StructType = Args.structTypeOrElse(args, "dataschema2", StructType(List(
    StructField("ra", DoubleType, false),
    StructField("dec", DoubleType, false))))

  val lowlevel: Int = Args.intOrElse(args, "lowlevel", 10)
  val highlevel: Int = Args.intOrElse(args, "highlevel", 17)
  val randfactor: Int = Args.intOrElse(args, "randfactor", 10)

  val jkcount: Int = if (jackknife) Args.intOrElse(args, "jkcount", 16) else 0
  val masklevel: Int = Args.intOrElse(args, "masklevel", 9)
  val masklowlevel: Int = Args.intOrElse(args, "masklowlevel", 3)

  val lowpixcount = HealpixBase.order2Npix(lowlevel)
  val highpixcount = HealpixBase.order2Npix(highlevel)

  val cores: Int = Args.intOrElse(args, "cores", 8)
  val partitions: Int = Args.intOrElse(args, "partitions", lowpixcount.toInt)

  val maskconfidence: Double = Args.doubleOrElse(args, "maskconfidence", 0.9)
  val datasize: Long = Args.longOrElse(args, "datasize", 1000L)

  val ringfactor: Int = Args.intOrElse(args, "ringfactor", 32)

  val bins: Int = Args.intOrElse(args, "bins", 50)
  val minradiusdeg: Double = Args.doubleOrElse(args, "minradius", 1.0)
  val maxradiusdeg: Double = Args.doubleOrElse(args, "maxradius", 15.0)
  val scale: String = Args.stringOrElse(args, "scale", "sqrt")

  /*val bins: Int = Args.intOrElse(args, "bins", 10)
  val minradiusdeg: Double = Args.doubleOrElse(args, "minradius", 0.05)
  val maxradiusdeg: Double = Args.doubleOrElse(args, "maxradius", 10.0)
  val scale: String = Args.stringOrElse(args, "scale", "log10")*/

  /*val bins: Int = Args.intOrElse(args, "bins", 3)
  val minradiusdeg: Double = Args.doubleOrElse(args, "minradius", 0.1)
  val maxradiusdeg: Double = Args.doubleOrElse(args, "maxradius", 1.0)
  val scale: String = Args.stringOrElse(args, "scale", "sqrt")*/

  /*val bins: Int = 16
  val minradiusdeg: Double = Args.doubleOrElse(args, "minradius", 1.0 / 60.0)
  val maxradiusdeg: Double = Args.doubleOrElse(args, "maxradius", 6.0)
  val scale: String = "log10" // FOR ASTROML TEST*/

  /*val bins: Int = 16
  val minradiusdeg: Double = Args.doubleOrElse(args, "minradius", 0.0)
  val maxradiusdeg: Double = Args.doubleOrElse(args, "maxradius", 1.0 / 3600.0)
  val scale: String = "log10"*/

  val binner: Binner = Binner.create(minradiusdeg, maxradiusdeg, bins, scale)

  val minradius = binner.minRadius
  val maxradius = binner.maxRadius

  val loglevel: String = Args.stringOrElse(args, "loglevel", "ERROR")

  val printrealfactor: Boolean = Args.booleanOrElse(args, "printrealfactor", false)
  val printprogress: Boolean = Args.booleanOrElse(args, "printprogress", true)
  val printbincenters: Boolean = Args.booleanOrElse(args, "printbincenters", true)
  val printbins: Boolean = Args.booleanOrElse(args, "printbins", true)
  val printnummatches: Boolean = Args.booleanOrElse(args, "printnummatches", true)
  val printinfo: Boolean = Args.booleanOrElse(args, "printinfo", true)

  val resultdir: String = "file:///C://Data/Crossmatch/output_euclid"

  def this(args: Array[String]) {
    this(args.zipWithIndex.filter(p => p._2 % 2 == 0).map(p => p._1).zip(args.zipWithIndex.filter(p => p._2 % 2 != 0).map(p => p._1)).toMap)
  }

}

object Args {
  def booleanOrElse(args: Map[String, String], name: String, default: Boolean) =
    try { args(name).toBoolean } catch { case _: Throwable => default }

  def stringOrElse(args: Map[String, String], name: String, default: String) =
    try { args(name) } catch { case _: Throwable => default }

  def intOrElse(args: Map[String, String], name: String, default: Int) =
    try { args(name).toInt } catch { case _: Throwable => default }

  def longOrElse(args: Map[String, String], name: String, default: Long) =
    try { args(name).toLong } catch { case _: Throwable => default }

  def doubleOrElse(args: Map[String, String], name: String, default: Double) =
    try { args(name).toDouble } catch { case _: Throwable => default }

  def structTypeOrElse(args: Map[String, String], name: String, default: StructType) = {
    try {
      StructType(
        args(name)
          .split(",")
          .map(_.split(":"))
          .map(p => StructField(
            p(0),
            p(1) match {
              case "int" => IntegerType
              case "long" => LongType
              case "float" => FloatType
              case "double" => DoubleType
              case "bool" => BooleanType
              case _ => StringType
            },
            nullable = false)))
    } catch { case _: Throwable => println("schema not recognised: " + name); default }
  }

}