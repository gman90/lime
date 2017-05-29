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
    nearest_RDD.collect().foreach(println)

    ReldistStatistic(0, 0, 0, 0)

  }

}

private case class ReldistStatistic(intersectLength: Long,
                                    unionLength: Long,
                                    jaccardDist: Double,
                                    nIntersections: Long) extends StatisticResult {

  override def toString(): String = {

    "intersection\tunion-intersection\tjaccard\tn_intersections\n" +
      s"$intersectLength\t$unionLength\t$jaccardDist\t$nIntersections"
  }
}