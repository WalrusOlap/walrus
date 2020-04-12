package org.pcg.walrus.common.util

import scala.collection.Seq

/**
  * common math functions
  */
object MathUtil {

  // median number
  def mean(s: Seq[Double]): Double  = s.sum / s.length

  // median number
  def median(s: Seq[Double]): Double  =
  {
    val (lower, upper) = s.sortWith(_<_).splitAt(s.size / 2)
    if (s.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
  }

  // normalization function
  def normalize(x: Seq[Double], func: String = "linearScale"): Seq[Double] = {
    func match {
      case "zScore" => {
        val mean = x.sum/x.length
        val stdev = stdDev(x)
        x.map { y => (y - mean) / stdev }
      }
      case "minMax" => x.map { y => (y - x.min) / (x.max - x.min) }
      case "linearScale" => x.map { y => y / x.max } // default: linear scale
      case _ => null
    }
  }

  // stdDev function
  def stdDev(x: Seq[Double]): Double = Math.sqrt((x.map( _ - mean(x)).map(t => t*t).sum) / x.length)
}