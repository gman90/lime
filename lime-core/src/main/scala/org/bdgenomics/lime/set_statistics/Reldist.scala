package org.bdgenomics.lime.set_statistics

import org.apache.spark.rdd.RDD
import org.bdgenomics.lime.set_theory.SingleNearest
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.adam.models.ReferenceRegion

import scala.reflect.ClassTag

class Relative[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, Feature)],
                                         rightRdd: RDD[(ReferenceRegion, Feature)],
                                         leftPartitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                         rightPartitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]]) extends Statistic {

  def compute(): StatisticResult = {

    val nearest_RDD = new SingleNearest(leftRdd, rightRdd, leftPartitionMap).compute()
    val resultRdd = nearest_RDD.map(f => {
      (f._1, List(ReferenceRegion(f._2._2.getContigName, f._2._2.getStart, f._2._2.getEnd)))
    }).reduceByKey((a, b) => (a ++ b)).map(f => {
      val nearest = if (f._2.size < 2) {
        new ReferenceRegion("", 0, 0) :: f._2
      } else {
        f._2
      }
      val numer = nearest.map(g => {
        Math.abs((f._1.start + (f._1.end - f._1.start) / 2) - (g.start + (g.end - g.start) / 2))
      }).min

      val denom = nearest.foldLeft(0.0)((a, b) => Math.abs(a - (b.start + (b.end - b.start) / 2)))
      if ((f._1.start) == 135453) {
        println(numer)
        println(denom)

      }
      (f._1, numer / denom)

    })

    resultRdd.collect().foreach(println)

    ReldistStatistic(resultRdd)

  }
}

private case class ReldistStatistic(resultRdd: RDD[(ReferenceRegion, Double)]) extends StatisticResult {

  override def toString(): String = {
    resultRdd.toString()
  }
}