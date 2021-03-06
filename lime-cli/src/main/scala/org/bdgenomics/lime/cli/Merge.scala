package org.bdgenomics.lime.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.lime.set_theory.DistributedMerge
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.Argument

object Merge extends BDGCommandCompanion {
  val commandName = "merge"
  val commandDescription = "Merges the regions in a file."

  def apply(cmdLine: Array[String]) = {
    new Merge(Args4j[MergeArgs](cmdLine))
  }

  class MergeArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
    @Argument(required = true,
      metaVar = "INPUT",
      usage = "The file to merge",
      index = 0)
    var input: String = null

    override var sortFastqOutput: Boolean = false
    override var asSingleFile: Boolean = false
    override var deferMerging: Boolean = false
    override var outputPath: String = ""
    override var disableFastConcat: Boolean = false
  }

  class Merge(protected val args: MergeArgs) extends BDGSparkCommand[MergeArgs] {
    val companion = Merge

    def run(sc: SparkContext) = {
      val leftGenomicRDD = sc.loadBed(args.input).repartitionAndSort()
      val leftGenomicRDDKeyed = leftGenomicRDD.rdd.map(f => (ReferenceRegion.stranded(f), f))

      DistributedMerge(leftGenomicRDDKeyed,
        leftGenomicRDD.partitionMap.get)
        .compute()
        .collect.foreach(println)
    }
  }

}
