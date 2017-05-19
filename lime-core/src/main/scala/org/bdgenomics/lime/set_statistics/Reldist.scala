package org.bdgenomics.lime.set_statistics

import org.apache.spark.rdd.RDD
import org.bdgenomics.lime.set_theory.{ Closest, SingleClosest }
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.adam.models.ReferenceRegion

import scala.reflect.ClassTag

class Reldist[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, Feature)],
                                        rightRdd: RDD[(ReferenceRegion, Feature)],
                                        leftPartitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                        rightPartitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]]) extends Statistic {

  def compute(): StatisticResult = {

    val closest_RDD = new SingleClosest(leftRdd, rightRdd, leftPartitionMap).compute().map(f => (ReferenceRegion(f._2._1.getContigName, f._2._1.getStart, f._2._1.getEnd),
      ReferenceRegion(f._2._2.getContigName, f._2._2.getStart, f._2._2.getEnd)))
      .map(f => (f._1,List(f._2.start + (f._2.end - f._2.start)/2)))
      .reduceByKey((_++_))
      .map(f=>(f._1,f._2.sorted))
      .map(f=>{
        val current_mid = f._1.start + (f._1.end - f._1.start)/2
        val left =
        if (f._2.filter(_ <= current_mid).isDefinedAt()){
          f._2.filter(_ <= current_mid).get
        }
        else
          0
        val right =
          if (f._2.filter(_ > current_mid).isDefinedAt()){
            f._2.filter(_ > current_mid).get
          }
          else
            0

          if (left !=  0 || right != 0)
            {
              (f._1 , Math.min(Math.abs(current_mid - left),Math.abs(right - current_mid))/ (right - left))
            }



      }



      )



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