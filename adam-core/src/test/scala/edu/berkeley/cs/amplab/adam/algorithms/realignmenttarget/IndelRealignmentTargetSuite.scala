
package edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, ADAMRecord}
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.SparkContext._
import parquet.filter.UnboundRecordFilter
import scala.collection.immutable.{NumericRange, TreeSet}
import edu.berkeley.cs.amplab.adam.rdd.RealignIndels

class IndelRealignmentTargetSuite extends SparkFunSuite {

  // Note: this can't be lazy vals because Spark won't find the RDDs after the first test
  def mason_reads: RDD[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("small_realignment_targets.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
  }

  def mason_rods : RDD[Seq[ADAMPileup]] = {
    mason_reads.adamRecords2Pileup()
      .groupBy(_.getPosition) // this we just do to match behaviour in IndelRealignerTargetFinder
      .sortByKey(ascending = true, numPartitions = 1)
      .map(_._2)
  }

  def artificial_reads: RDD[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("artificial.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
  }

  def artificial_rods : RDD[Seq[ADAMPileup]] = {
    artificial_reads.adamRecords2Pileup()
      .groupBy(_.getPosition) // this we just do to match behaviour in IndelRealignerTargetFinder
      .sortByKey(ascending = true, numPartitions = 1)
      .map(_._2)
  }

  sparkTest("checking simple ranges") {
    val range1 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](1, 4, 1),
      new NumericRange.Inclusive[Long](1, 10, 1)
    )
    val range2 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](1, 4, 1),
      new NumericRange.Inclusive[Long](40, 50, 1)
    )
    val range3 : SNPRange = new SNPRange(5, new NumericRange.Inclusive[Long](40, 50, 1))

    assert(range1 != range2)
    assert(range1.compareRange(range2) === 0)
    assert(range1.compare(range2) === 0)
    assert(range1.compareReadRange(range2) != 0)
    assert(range2.compareReadRange(range3) === 0)
  }

  sparkTest("checking simple realignment target") {
    val range1 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](1, 4, 1),
      new NumericRange.Inclusive[Long](1, 10, 1)
    )
    val range2 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](1, 4, 1),
      new NumericRange.Inclusive[Long](40, 50, 1)
    )
    val range3 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](6, 14, 1),
      new NumericRange.Inclusive[Long](80, 90, 1)
    )
    val range4 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](2, 4, 1),
      new NumericRange.Inclusive[Long](60, 70, 1)
    )

    val indelRanges1 = (range1 :: range2 :: List()).toSet
    val target1 = new IndelRealignmentTarget(indelRanges1, Set.empty[SNPRange])
    val indelRanges2 = (range3 :: range4 :: List()).toSet
    val target2 = new IndelRealignmentTarget(indelRanges2, Set.empty[SNPRange])
    assert(target1.readRange.start === 1)
    assert(target1.readRange.end === 50)
    assert(TargetOrdering.overlap(target1, target1) === true)
    assert(TargetOrdering.overlap(target1, target2) === false)
    assert(target2.getReadRange().start === 60)
    assert(target2.getReadRange().end === 90)
  }

  sparkTest("extracting matches, mismatches and indels from mason reads") {

    val extracted_rods : RDD[Tuple3[Seq[ADAMPileup], Seq[ADAMPileup], Seq[ADAMPileup]]] =
        mason_rods.map(x => Tuple3(IndelRealignmentTarget.extractIndels(x), IndelRealignmentTarget.extractMatches(x), IndelRealignmentTarget.extractMismatches(x)))
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

  sparkTest("creating targets for artificial reads") {
    val artificial_pileup = artificial_rods.collect()
    assert(artificial_pileup.size > 0)
    val targets_collected : Array[IndelRealignmentTarget] = RealignmentTargetFinder(artificial_reads).toArray
    // there are no SNPs in the artificial reads
    val only_SNPs = targets_collected.filter(_.getSNPSet() != Set.empty)
    // TODO: it seems that mismatches all create separate SNP targets?
    //assert(only_SNPs.size == 0)
    // there are two indels (deletions) in the reads
    val only_indels = targets_collected.filter(_.getIndelSet() != Set.empty)
    assert(only_indels.size === 1)
    assert(only_indels.head.getIndelSet().size == 2)
    assert(only_indels.head.getReadRange().start === 5)
    assert(only_indels.head.getReadRange().end === 94)
    val indelsets = only_indels.head.getIndelSet().toArray
    // NOTE: this assumes the set is in sorted order, which seems to be the case
    assert(indelsets(0).getIndelRange().start === 34)
    assert(indelsets(0).getIndelRange().start === 43)
    assert(indelsets(1).getIndelRange().start === 54)
    assert(indelsets(1).getIndelRange().start === 63)
    //
  }

  sparkTest("creating SNP targets for mason reads") {
    val targets_collected : Array[IndelRealignmentTarget] = RealignmentTargetFinder(mason_reads).toArray
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

  sparkTest("creating indel targets for mason reads") {
    object IndelRangeOrdering extends Ordering[IndelRange] {
      def compare(x: IndelRange, y: IndelRange): Int = x.getIndelRange().start compare y.getIndelRange().start
    }

    val targets_collected : Array[IndelRealignmentTarget] = RealignmentTargetFinder(mason_reads).toArray
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