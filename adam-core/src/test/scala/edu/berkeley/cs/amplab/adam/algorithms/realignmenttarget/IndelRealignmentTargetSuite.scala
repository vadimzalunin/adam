
package edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, ADAMRecord}
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.SparkContext._
import parquet.filter.UnboundRecordFilter

class IndelRealignmentTargetSuite extends SparkFunSuite {

  def rods : RDD[Seq[ADAMPileup]] = {
    val path = ClassLoader.getSystemClassLoader.getResource("small_realignment_targets.bam").getFile
    val reads : RDD[ADAMRecord] = sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
    reads.adamRecords2Pileup()
      .groupBy(_.getPosition) // this we just do to match behaviour in IndelRealignerTargetFinder
      .sortByKey(ascending = true, numPartitions = 1)
      .map(_._2)
  }

  sparkTest("extracting match, mismatch and indel") {
    val extracted_rods : RDD[Tuple3[Seq[ADAMPileup], Seq[ADAMPileup], Seq[ADAMPileup]]] =
        rods.map(x => Tuple3(IndelRealignmentTarget.extractIndels(x), IndelRealignmentTarget.extractMatches(x), IndelRealignmentTarget.extractMismatches(x)))
    val extracted_rods_collected : Array[Tuple3[Seq[ADAMPileup], Seq[ADAMPileup], Seq[ADAMPileup]]] = extracted_rods.collect()

    // the first read has CIGAR 100M and MD 92T7
    assert(extracted_rods_collected.slice(0, 100).forall(x => x._1.length == 0))
    assert(extracted_rods_collected.slice(0, 92).forall(x => x._2.length == 1))
    assert(extracted_rods_collected.slice(0, 92).forall(x => x._3.length == 0))
    assert(extracted_rods_collected(92)._2.length === 0)
    assert(extracted_rods_collected(92)._3.length === 1)
    assert(extracted_rods_collected.slice(93, 100).forall(x => x._2.length == 1))
    assert(extracted_rods_collected.slice(93, 100).forall(x => x._3.length == 0))
    // the second read has CIGAR 32M1D33M1I34M and MD 0G24A6^T67
    assert(extracted_rods_collected.slice(100, 132).forall(x => x._1.length == 0))
    // first the SNP at the beginning
    assert(extracted_rods_collected(100)._2.length === 0)
    assert(extracted_rods_collected(100)._3.length === 1)
    // now a few matches
    assert(extracted_rods_collected.slice(101, 125).forall(x => x._2.length == 1))
    assert(extracted_rods_collected.slice(101, 125).forall(x => x._3.length == 0))
    // another SNP
    assert(extracted_rods_collected(125)._2.length === 0)
    assert(extracted_rods_collected(125)._3.length === 1)
    // a few more matches
    assert(extracted_rods_collected.slice(126, 132).forall(x => x._2.length == 1))
    assert(extracted_rods_collected.slice(126, 132).forall(x => x._3.length == 0))
    // now comes the deletion of T
    assert(extracted_rods_collected(132)._1.length === 1)
    assert(extracted_rods_collected(132)._2.length === 0)
    assert(extracted_rods_collected(132)._3.length === 0)
    // now 33 more matches
    assert(extracted_rods_collected.slice(133, 166).forall(x => x._1.length == 0))
    assert(extracted_rods_collected.slice(133, 166).forall(x => x._2.length == 1))
    assert(extracted_rods_collected.slice(133, 166).forall(x => x._3.length == 0))
    // now one insertion
    assert(extracted_rods_collected(166)._1.length === 1)
    assert(extracted_rods_collected(166)._2.length === 1)
    assert(extracted_rods_collected(166)._3.length === 0)
    // TODO: add read with more insertion and few deletions
  }

  sparkTest("creating indel targets for small input") {
    val targets : RDD[IndelRealignmentTarget] =
      rods.map(IndelRealignmentTarget(_))
    val targets_collected = targets.collect()

    // first look at SNPs
    val only_SNPs = targets.filter(_.getSNPSet() != Set.empty).collect()
    assert(only_SNPs(0).getSNPSet().head.getSNPSite() === 701384)
    assert(only_SNPs(1).getSNPSet().head.getSNPSite() === 702257)
    assert(only_SNPs(2).getSNPSet().head.getSNPSite() === 702282)
    assert(only_SNPs(3).getSNPSet().head.getSNPSite() === 807733)
    // TODO: the 4th and 5th SNP looks strange/absent from samtools mpileup
    // so let's check on that later
    assert(only_SNPs(6).getSNPSet().head.getSNPSite() === 869673)
    Console.println("checked SNPs")
  }
}