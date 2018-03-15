package org.msu.astro.astrospark

abstract class Binner(protected val minRadiusDeg: Double, protected val maxRadiusDeg: Double, val binCount: Int) extends Serializable {

  val minRadius = math.toRadians(minRadiusDeg)
  val maxRadius = math.toRadians(maxRadiusDeg)

  val correctedMax = straightFunction(maxRadiusDeg)
  val correctedMin = straightFunction(minRadiusDeg)

  val bins = createBins()

  def createBins(): Array[Double] = {
    val tmpBins = Array.fill(binCount + 1) { 0.0 }
    for (i <- 0 to binCount) {
      tmpBins(i) = indexToDist(i)
    }
    return tmpBins
  }

  def distToIndex(dist: Double): Int = {
    return ((straightFunction(math.toDegrees(dist)) - correctedMin) * binCount / (correctedMax - correctedMin)).toInt
  }
  def indexToDist(index: Int): Double = {
    return math.toRadians(inverseFunction(correctedMin + (correctedMax - correctedMin) * index.toDouble / binCount))
  }

  def straightFunction(x: Double): Double
  def inverseFunction(x: Double): Double

}

object Binner {
  def create(minradius: Double, maxradius: Double, bins: Int, scale: String): Binner = {
    val binner = scale match {
      case "sqrt" => new SqrtBinner(minradius, maxradius, bins);
      case "linear" => new LinearBinner(minradius, maxradius, bins);
      case _ => new Log10Binner(minradius, maxradius, bins);
    }
    return binner
  }

  def create(args: Map[String, String]): Binner = create(args("minradius").toDouble, args("maxradius").toDouble, args("bins").toInt, args("scale"))
}

class LinearBinner(override val minRadiusDeg: Double, override val maxRadiusDeg: Double, override val binCount: Int) extends Binner(minRadiusDeg, maxRadiusDeg, binCount) {

  override def straightFunction(x: Double): Double = {
    return x
  }

  override def inverseFunction(x: Double): Double = {
    return x
  }

}

class SqrtBinner(override val minRadiusDeg: Double, override val maxRadiusDeg: Double, override val binCount: Int) extends Binner(minRadiusDeg, maxRadiusDeg, binCount) {

  override def straightFunction(x: Double): Double = {
    return math.sqrt(x)
  }

  override def inverseFunction(x: Double): Double = {
    return x * x
  }
}

class Log10Binner(override val minRadiusDeg: Double, override val maxRadiusDeg: Double, override val binCount: Int) extends Binner(minRadiusDeg, maxRadiusDeg, binCount) {

  override def straightFunction(x: Double): Double = {
    return math.log10(x)
  }

  override def inverseFunction(x: Double): Double = {
    return math.pow(10, x)
  }
}