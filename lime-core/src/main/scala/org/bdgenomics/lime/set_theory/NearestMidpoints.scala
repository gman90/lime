package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.lime.util.Partitioners.ReferenceRegionRangePartitioner
import org.bdgenomics.utils.interval.array.IntervalArray
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

sealed abstract class NearestMidpoints[T: ClassTag, U: ClassTag] extends SetTheoryBetweenCollections[T, U, T, U] {

  var leftNearest: ReferenceRegion = ReferenceRegion("", 0, 0)
  var rightNearest: ReferenceRegion = ReferenceRegion("", 0, 0)

  /**
   * The condition requirement here is that the first region be closer to the
   * second region than the current closest.
   *
   * @param firstRegion The region to test against.
   * @param secondRegion The region to test.
   * @param threshold The distance requirement for closest.
   * @return True if the threshold requirement is met.
   *         False if the threshold requirement is not met.
   */
  override protected def condition(firstRegion: ReferenceRegion,
                                   secondRegion: ReferenceRegion,
                                   threshold: Long = 0L): Boolean = {

    val currentMidpoint = firstRegion.start + (firstRegion.end - firstRegion.start) / 2
    val leftMidpoint = leftNearest.start + (leftNearest.end - leftNearest.start) / 2
    val rightMidpoint = rightNearest.start + (rightNearest.end - rightNearest.start) / 2
    val queryMidpoint = secondRegion.start + (secondRegion.end - secondRegion.start) / 2

    if (((queryMidpoint >= currentMidpoint) && (Math.abs(rightMidpoint - currentMidpoint) >= Math.abs(queryMidpoint - currentMidpoint))) ||
      ((queryMidpoint < currentMidpoint) && (Math.abs(leftMidpoint - currentMidpoint) >= Math.abs(queryMidpoint - currentMidpoint)))) {

      true
    } else
      false

  }
}

class SingleNearest[T: ClassTag, U: ClassTag](protected val leftRdd: RDD[(ReferenceRegion, T)],
                                              protected val rightRdd: RDD[(ReferenceRegion, U)],
                                              protected val partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                              protected val threshold: Long = 0L) extends NearestMidpoints[T, U] {

  /**
   * Prepares the two RDDs for the closest operation. Copartitions the right
   * according to the left. In the case that no data is assigned to a
   * partition, there is a second pass that duplicates the data on both
   * flanking nodes.
   *
   * @return A tuple containing:
   *         The left RDD, unchanged.
   *         The right RDD, copartitioned with the left.
   *         The original partition map.
   */
  override protected def prepare(): (RDD[(ReferenceRegion, T)], RDD[(ReferenceRegion, U)], Array[Option[(ReferenceRegion, ReferenceRegion)]]) = {

    val adjustedPartitionMapWithIndex = partitionMap
      // the zipWithIndex gives us the destination partition ID
      .zipWithIndex
      .filter(_._1.nonEmpty)
      .map(f => (f._1.get, f._2))
      .map(g => {
        // in the case where we span multiple referenceNames
        if (g._1._1.referenceName != g._1._2.referenceName) {
          // create a ReferenceRegion that goes to the end of the chromosome
          (ReferenceRegion(
            g._1._1.referenceName,
            g._1._1.start,
            g._1._1.end),
            g._2)
        } else {
          // otherwise we just have the ReferenceRegion span from partition
          // start to end
          (ReferenceRegion(
            g._1._1.referenceName,
            g._1._1.start,
            g._1._2.end),
            g._2)
        }
      })

    val partitionMapIntervals = IntervalArray(
      adjustedPartitionMapWithIndex,
      adjustedPartitionMapWithIndex.maxBy(_._1.width)._1.width,
      sorted = true)

    val assignedRightRdd = {
      val firstPass = rightRdd.mapPartitions(iter => {
        iter.flatMap(f => {
          val rangeOfHits = partitionMapIntervals.get(f._1, requireOverlap = false)
          rangeOfHits.map(g => ((f._1, g._2), f._2))
        })
      }, preservesPartitioning = true)

      val partitionsWithoutData =
        partitionMap.indices.filterNot(firstPass.map(_._1._2).distinct().collect.contains)

      val partitionsToSend = partitionsWithoutData.foldLeft(List.empty[List[Int]])((b, a) => {
        if (b.isEmpty) {
          List(List(a))
        } else if (a == b.last.last + 1) {
          b.dropRight(1).:+(b.last.:+(a))
        } else {
          b.:+(List(a))
        }
      }).flatMap(f => List((f.head - 1, f.length), (f.last + 1, -1 * f.length)))

      firstPass.flatMap(f => {
        val index = partitionsToSend.indexWhere(_._1 == f._1._2)
        if (index < 0) {
          List(f)
        } else {
          if (partitionsToSend(index)._2 < 0) {
            (partitionsToSend(index)._2 to 0)
              .map(g => ((f._1._1, f._1._2 + g), f._2))
          } else {
            (0 to partitionsToSend(index)._2)
              .map(g => ((f._1._1, f._1._2 + g), f._2)) ++ {
                if (index == partitionsToSend.lastIndexWhere(_._1 == f._1._2)) {
                  List()
                } else {
                  val endIndex = partitionsToSend.lastIndexWhere(_._1 == f._1._2)
                  (partitionsToSend(endIndex)._2 to -1)
                    .map(g => ((f._1._1, f._1._2 + g), f._2))
                }
              }
          }
        }
      })
    }
    val preparedRightRdd =
      assignedRightRdd
        .repartitionAndSortWithinPartitions(
          new ReferenceRegionRangePartitioner(partitionMap.length))
        // return to an RDD[(ReferenceRegion, T)], removing the partition ID
        .map(f => (f._1._1, f._2))
    (leftRdd, preparedRightRdd, partitionMap)
  }

  /**
   * The primitive to be computed in the case of closest is simply to return
   * the firstRegion.
   *
   * @param firstRegion The first region to compute.
   * @param secondRegion The second region to compute.
   * @param threshold The distance requirement for closest.
   * @return The first region.
   */
  override protected def primitive(firstRegion: ReferenceRegion,
                                   secondRegion: ReferenceRegion,
                                   threshold: Long = 0L): ReferenceRegion = {

    firstRegion
  }

  /**
   * Prunes the cache of all regions that are no longer candidates for the
   * closest region.
   *
   * @param cachedRegion The current region in the cache.
   * @param to The region that is compared against.
   * @return True for regions that should be removed.
   *         False for all regions that should remain in the cache.
   */
  override protected def pruneCacheCondition(cachedRegion: ReferenceRegion,
                                             to: ReferenceRegion): Boolean = {

    if (cachedRegion.referenceName != to.referenceName) {
      true
    } else {

      val cachedMidpoint = cachedRegion.start + (cachedRegion.end - cachedRegion.start) / 2
      val toMidpoint = to.start + (to.end - to.start) / 2
      val leftMidpoint = leftNearest.start + (leftNearest.end - leftNearest.start) / 2
      val rightMidpoint = rightNearest.start + (rightNearest.end - rightNearest.start) / 2

      if (((cachedMidpoint >= toMidpoint) && (Math.abs(rightMidpoint - cachedMidpoint) < Math.abs(toMidpoint - cachedMidpoint))) ||
        ((cachedMidpoint < toMidpoint) && (Math.abs(cachedMidpoint - leftMidpoint) < Math.abs(cachedMidpoint - toMidpoint)))) {
        true
      } else
        false

    }

  }

  /**
   * Advances the cache to add the closest region to the cache.
   *
   * @param candidateRegion The current candidate region.
   * @param until The region to compare against.
   * @return True for all regions to be added to the cache.
   *         False for regions that should not be added to the cache.
   */
  override protected def advanceCacheCondition(candidateRegion: ReferenceRegion,
                                               until: ReferenceRegion): Boolean = {

    val leftMidpoint = leftNearest.start + (leftNearest.end - leftNearest.start) / 2
    val rightMidpoint = rightNearest.start + (rightNearest.end - rightNearest.start) / 2
    val candidateMidpoint = candidateRegion.start + (candidateRegion.end - candidateRegion.start) / 2
    val untilMidpoint = until.start + (until.end - until.start) / 2

    if (candidateRegion.referenceName != until.referenceName) {

      false
    } else if ((candidateMidpoint >= untilMidpoint) && (Math.abs(rightMidpoint - untilMidpoint) >= Math.abs(untilMidpoint - candidateMidpoint))) {

      rightNearest = candidateRegion
      true
    } else if (((candidateMidpoint < untilMidpoint) && (Math.abs(untilMidpoint - leftMidpoint) >= Math.abs(untilMidpoint - candidateMidpoint)))) {
      leftNearest = candidateRegion

      true

    } else {

      false

    }
  }

  /**
   * Processes the hits and pairs the current left region with the closest
   * region on the right.
   *
   * @param current The current left row, keyed by the ReferenceRegion.
   * @param cache The cache of potential hits.
   * @return An iterator containing the current left with the closest region.
   */
  override protected def processHits(current: (ReferenceRegion, T),
                                     cache: ListBuffer[(ReferenceRegion, U)]): Iterator[(ReferenceRegion, (T, U))] = {

    val (currentRegion, currentValue) = current
    cache.filter(f => {
      val (rightRegion, _) = f
      condition(currentRegion, rightRegion)
    }).map(f => {
      val (rightRegion, rightValue) = f
      (primitive(currentRegion, rightRegion), (currentValue, rightValue))
    }).toIterator
  }
}

