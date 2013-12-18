
package edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, ADAMRecord}
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.SparkContext._
import parquet.filter.UnboundRecordFilter
import scala.collection.immutable.TreeSet
import edu.berkeley.cs.amplab.adam.rdd.RealignIndels

class IndelRealignmentTargetSuite extends SparkFunSuite {

  def reads: RDD[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("small_realignment_targets.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
  }

  def rods : RDD[Seq[ADAMPileup]] = {
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
    // TODO: add read with more insertions, overlapping reads
  }

  sparkTest("creating SNP targets for small input") {
    val targets_collected : Array[IndelRealignmentTarget] = RealignmentTargetFinder(reads).toArray
    assert(targets_collected.size > 0)

    // first look at SNPs
    val only_SNPs = targets_collected.filter(_.getSNPSet() != Set.empty) //.collect()
    // the first read has a single SNP
    assert(only_SNPs(0).getSNPSet().size === 1)
    assert(only_SNPs(0).getSNPSet().head.getSNPSite() === 701384)
    // the second read has two SNPS
    assert(only_SNPs(1).getSNPSet().size === 2)
    assert(only_SNPs(1).getSNPSet().head.getSNPSite() === 702257)
    assert(only_SNPs(1).getSNPSet().toIndexedSeq(1).getSNPSite() === 702282)
    // the third has a single SNP
    assert(only_SNPs(2).getSNPSet().size === 1)
    assert(only_SNPs(2).getSNPSet().head.getSNPSite() === 807733)
    // the last read has two SNPs
    assert(only_SNPs(4).getSNPSet().size === 2)
    assert(only_SNPs(4).getSNPSet().head.getSNPSite() === 869572)
    assert(only_SNPs(4).getSNPSet().toIndexedSeq(1).getSNPSite() === 869673)
  }

  sparkTest("creating indel targets for small input") {
    object IndelRangeOrdering extends Ordering[IndelRange] {
      def compare(x: IndelRange, y: IndelRange): Int = x.getIndelRange().start compare y.getIndelRange().start
    }

    val targets_collected : Array[IndelRealignmentTarget] = RealignmentTargetFinder(reads).toArray
    assert(targets_collected.size > 0)

    val only_indels =  targets_collected.filter(_.getIndelSet() != Set.empty)
    // the first read has no indels
    // the second read has a one-base deletion and a one-base insertion
    assert(only_indels(0).getIndelSet().size === 2)
    val tmp1 = new TreeSet()(IndelRangeOrdering).union(only_indels(0).getIndelSet())
    assert(tmp1.toIndexedSeq(0).getIndelRange().start == 702289  && tmp1.toIndexedSeq(0).getIndelRange().end == 702289)
    assert(tmp1.toIndexedSeq(1).getIndelRange().start == 702323 && tmp1.toIndexedSeq(1).getIndelRange().end == 702323)
    // the third read has a one base deletion
    assert(only_indels(1).getIndelSet().size === 1)
    assert(only_indels(1).getIndelSet().head.getIndelRange().start == 807755 && only_indels(1).getIndelSet().head.getIndelRange().end == 807755)
    // read 7 has 4 deletions
    assert(only_indels(5).getIndelSet().size === 4)
    assert(only_indels(5).getIndelSet().head.getIndelRange().start == 869644 && only_indels(5).getIndelSet().head.getIndelRange().end == 869647)
  }
}