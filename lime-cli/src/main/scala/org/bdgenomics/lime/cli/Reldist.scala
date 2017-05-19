package org.bdgenomics.lime.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{ ADAMSaveAnyArgs, GenomicRDD, feature }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.Argument
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.lime.set_statistics.JaccardDistance

object Reldist extends BDGCommandCompanion {
  val commandName = "reldist"
  val commandDescription = "Compute  reldist between two inputs"

  def apply(cmdLine: Array[String]) = {
    new Reldist(Args4j[ReldistArgs](cmdLine))
  }

  class ReldistArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
    @Argument(required = true,
      metaVar = "INPUT1",
      usage = "The first file for reldist",
      index = 0)
    var leftInput: String = null
    @Argument(required = true,
      metaVar = "INPUT2",
      usage = "The second file for reldist",
      index = 1)
    var rightInput: String = null

    override var sortFastqOutput: Boolean = false
    override var asSingleFile: Boolean = false
    override var deferMerging: Boolean = false
    override var outputPath: String = ""
    override var disableFastConcat: Boolean = false
  }

  class Reldist(protected val args: ReldistArgs) extends BDGSparkCommand[ReldistArgs] {
    val companion = Reldist

    def run(sc: SparkContext) {

      val leftGenomicRDD = sc.loadFeatures(args.leftInput).sortLexicographically()
      val leftGenomicRDDKeyed = leftGenomicRDD.rdd.map(f => (ReferenceRegion.unstranded(f), f))
      val rightGenomicRdd = sc.loadFeatures(args.rightInput).sortLexicographically()
      val rightGenomicRDDKeyed = rightGenomicRdd.rdd.map(f => (ReferenceRegion.unstranded(f), f))

      val rel_dist = new JaccardDistance(leftGenomicRDDKeyed, rightGenomicRDDKeyed, leftGenomicRDD.optPartitionMap.get,
        rightGenomicRdd.optPartitionMap.get).compute()
      println(jaccard_dist)

    }
  }

}
