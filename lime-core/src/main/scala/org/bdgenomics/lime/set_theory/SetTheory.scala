package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.lime.util.Partitioners.ReferenceRegionRangePartitioner
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

sealed abstract class SetTheory extends Serializable {

  protected val partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]]
  protected val threshold: Long

  /**
   * The primitive operation for intersection, computes the primitive of the
   * two regions.
   *
   * @param firstRegion The first region for the primitive.
   * @param secondRegion The second region for the primitive.
   * @param distanceThreshold The threshold for the primitive.
   * @return The computed primitive for the two regions.
   */
  protected def primitive(firstRegion: ReferenceRegion,
                          secondRegion: ReferenceRegion,
                          distanceThreshold: Long = 0L): ReferenceRegion

  /**
   * The condition that should be met in order for the primitive to be
   * computed.
   *
   * @param firstRegion The region to test against.
   * @param secondRegion The region to test.
   * @param distanceThreshold The threshold for the primitive.
   * @return True if the threshold requirement is met.
   *         False if the threshold requirement is not met.
   */
  protected def condition(firstRegion: ReferenceRegion,
                          secondRegion: ReferenceRegion,
                          distanceThreshold: Long = 0L): Boolean

  /**
   * Gets the partition bounds from a ReferenceRegion keyed Iterator
   *
   * @param iter The data on a given partition. ReferenceRegion keyed
   * @return The bounds of the ReferenceRegions on that partition, in an Iterator
   */
  protected def getRegionBoundsFromPartition[X](iter: Iterator[(ReferenceRegion, X)]): Iterator[Option[(ReferenceRegion, ReferenceRegion)]] = {
    if (iter.isEmpty) {
      // This means that there is no data on the partition, so we have no bounds
      Iterator(None)
    } else {
      val firstRegion = iter.next
      val lastRegion =
        if (iter.hasNext) {
          // we have to make sure we get the full bounds of this partition, this
          // includes any extremely long regions. we include the firstRegion for
          // the case that the first region is extremely long
          (iter ++ Iterator(firstRegion)).maxBy(f => (f._1.referenceName, f._1.end, f._1.start))
          // only one record on this partition, so this is the extent of the bounds
        } else {
          firstRegion
        }
      Iterator(Some((firstRegion._1, lastRegion._1)))
    }
  }
}

/**
 * The parent class for all inter-collection set theory operations.
 *
 * @tparam T The left side row data.
 * @tparam U The right side row data.
 * @tparam RT The return type for the left side row data.
 * @tparam RU The return type for the right side row data.
 */
abstract class SetTheoryBetweenCollections[T: ClassTag, U: ClassTag, RT, RU] extends SetTheory {

  protected val leftRdd: RDD[(ReferenceRegion, T)]
  protected val rightRdd: RDD[(ReferenceRegion, U)]

  // Holds candidates from the right side.
  private val cache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty[(ReferenceRegion, U)]

  /**
   * Processes all hits from the cache and creates an iterator for the current
   * left based on the primitive operation.
   *
   * @param current The current left row, keyed by the ReferenceRegion.
   * @param cache The cache of potential hits.
   * @return An iterator containing all processed hits.
   */
  protected def processHits(current: (ReferenceRegion, T),
                            cache: ListBuffer[(ReferenceRegion, U)]): Iterator[(ReferenceRegion, (RT, RU))]

  /**
   * The condition by which a candidate is removed from the cache.
   *
   * @see pruneCache
   * @param cachedRegion The current region in the cache.
   * @param to The region that is compared against.
   * @return True for regions that should be removed.
   *         False for all regions that should remain in the cache.
   */
  protected def pruneCacheCondition(cachedRegion: ReferenceRegion,
                                    to: ReferenceRegion): Boolean

  /**
   * The condition by which a candidate region is added to the cache.
   *
   * @see advanceCache
   * @param candidateRegion The current candidate region.
   * @param until The region to compare against.
   * @return True for all regions to be added to the cache.
   *         False for regions that should not be added to the cache.
   */
  protected def advanceCacheCondition(candidateRegion: ReferenceRegion,
                                      until: ReferenceRegion): Boolean

  /**
   * Partitions and prepares the RDDs according to the left
   */
  protected def prepare(): (RDD[(ReferenceRegion, T)], RDD[(ReferenceRegion, U)], Array[Option[(ReferenceRegion, ReferenceRegion)]])

  /**
   * Prunes the cache based on the condition set in pruneCacheCondition.
   *
   * @see pruneCacheCondition
   * @param to The region to prune against.
   */
  private def pruneCache(to: ReferenceRegion) = {

    cache.trimStart({
      val index = cache.indexWhere(f => !pruneCacheCondition(f._1, to))
      if (index <= 0) {
        0
      } else {
        index
      }
    })
  }

  /**
   * Advances the cache based on the condition set in advanceCacheCondition
   *
   * @see advanceCacheCondition
   * @param right The right buffered iterator to pull from.
   * @param until The region to compare against.
   */
  private def advanceCache(right: BufferedIterator[(ReferenceRegion, U)],
                           until: ReferenceRegion) = {
    while (right.hasNext && advanceCacheCondition(right.head._1, until)) {
      cache += right.next
    }
  }

  /**
   * Computes the set theory primitive for the two RDDs.
   *
   * @return An RDD resulting from the primitive operation.
   */
  def compute(): RDD[(ReferenceRegion, (RT, RU))] = {
    val (preparedLeftRdd, preparedRightRdd, preparedPartitionMap) = prepare()
    preparedLeftRdd.zipPartitions(preparedRightRdd)(makeIterator)
  }

  /**
   * Computes the set theory primitive for the two Iterators on each partition.
   *
   * @see processHits
   * @param leftIter The iterator for the left side of the primitive.
   * @param rightIter The iterator for the right side of the primitive.
   * @return The resulting Iterator based on the primitive operation.
   */
  protected def makeIterator(leftIter: Iterator[(ReferenceRegion, T)],
                             rightIter: Iterator[(ReferenceRegion, U)]): Iterator[(ReferenceRegion, (RT, RU))] = {

    val leftBuffered = leftIter.buffered
    val rightBuffered = rightIter.buffered

    leftBuffered.flatMap(f => {
      val (currentRegion, _) = f
      advanceCache(rightBuffered, currentRegion)
      pruneCache(currentRegion)
      processHits(f, cache)
    })
  }
}

abstract class SetTheoryWithSingleCollection[T: ClassTag] extends SetTheory {

  protected val rddToCompute: RDD[(ReferenceRegion, T)]

  /**
   * Post process the results based on each Set Theory primitive.
   *
   * @param rdd The RDD after computation is complete.
   * @return The RDD after post-processing.
   */
  protected def postProcess(rdd: RDD[(ReferenceRegion, Iterable[(ReferenceRegion, T)])]): RDD[(ReferenceRegion, Iterable[T])]

  def compute(): RDD[(ReferenceRegion, Iterable[T])] = {
    val localComputed = localCompute(rddToCompute.map(f => (f._1, Iterable((f._1, f._2)))), threshold)
    val finalComputed = interNodeCompute(localComputed, partitionMap, threshold, 2)
    postProcess(finalComputed)
  }

  protected def localCompute(rdd: RDD[(ReferenceRegion, Iterable[(ReferenceRegion, T)])],
                             distanceThreshold: Long): RDD[(ReferenceRegion, Iterable[(ReferenceRegion, T)])] = {

    rdd.mapPartitions(iter => {
      if (iter.hasNext) {
        iter.foldLeft(List(iter.next()))((b, a) => {
          if (condition(b.head._1, a._1)) {
            val t = b.head
            b.drop(1).+:((primitive(t._1, a._1), t._2 ++ a._2))
          } else {
            b.+:(a)
          }
        }).reverseIterator
      } else {
        Iterator()
      }
    })
  }

  /**
   * Computes the primitives between partitions.
   *
   * @param rdd The rdd to compute on.
   * @param partitionMap The current partition map for rdd.
   * @param distanceThreshold The distance threshold for the condition and primitive.
   * @param round The current round of computation in the recursion tree. Increments by a factor of 2 each round.
   * @return The computed rdd for this round.
   */
  @tailrec private def interNodeCompute(rdd: RDD[(ReferenceRegion, Iterable[(ReferenceRegion, T)])],
                                        partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                        distanceThreshold: Long,
                                        round: Int): RDD[(ReferenceRegion, Iterable[(ReferenceRegion, T)])] = {

    if (round > partitionMap.length) {
      return rdd
    }

    val partitionedRdd = rdd.mapPartitionsWithIndex((idx, iter) => {
      val indexWithinParent = idx % round
      val partnerPartition = {
        var i = 1
        if (idx > 0) {
          while (partitionMap(idx - i).isEmpty) {
            i += 1
          }
          idx - i
        } else {
          idx
        }
      }

      val partnerPartitionBounds = partitionMap(partnerPartition)

      iter.map(f => {
        val (region, value) = f
        if (indexWithinParent == round / 2 &&
          (region.covers(partnerPartitionBounds.get._2, distanceThreshold) ||
            region.compareTo(partnerPartitionBounds.get._2) <= 0)) {

          ((region, partnerPartition), value)
        } else {
          ((region, idx), value)
        }
      })
    }).repartitionAndSortWithinPartitions(new ReferenceRegionRangePartitioner(partitionMap.length))
      .map(f => (f._1._1, f._2))

    partitionedRdd.cache()

    val newPartitionMap = partitionedRdd.mapPartitions(iter => {
      getRegionBoundsFromPartition(iter)
    }).collect

    interNodeCompute(localCompute(partitionedRdd, distanceThreshold), newPartitionMap, distanceThreshold, round * 2)
  }
}
