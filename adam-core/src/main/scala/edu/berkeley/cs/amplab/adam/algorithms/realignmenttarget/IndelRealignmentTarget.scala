/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget

import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, ADAMRecord}
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import scala.collection.immutable.{HashSet, NumericRange}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.Logging

object TargetOrdering extends Ordering[IndelRealignmentTarget] {

  /**
   * Order two indel realignment targets by earlier starting position.
   *
   * @param a Indel realignment target to compare.
   * @param b Indel realignment target to compare.
   * @return Comparison done by starting position.
   */
  def compare (a: IndelRealignmentTarget, b: IndelRealignmentTarget) : Int = a.getReadRange.start compare b.getReadRange.start

  /**
   * Compares a read to an indel realignment target to see if it starts before the start of the indel realignment target.
   *
   * @param target Realignment target to compare.
   * @param read Read to compare.
   * @return True if start of read is before the start of the indel alignment target.
   */
  def lt (target: IndelRealignmentTarget, read: ADAMRecord) : Boolean = target.getReadRange.start < read.getStart

  /**
   * Check to see if an indel realignment target and a read are mapped over the same length.
   *
   * @param target Realignment target to compare.
   * @param read Read to compare.
   * @return True if read alignment span is identical to the target span. 
   */
  def equals (target: IndelRealignmentTarget, read: ADAMRecord) : Boolean = {
    (target.getReadRange.start == read.getStart) && (target.getReadRange.end == read.end.get)
  }

  /**
   * Compares two indel realignment targets to see if they overlap.
   *
   * @param a Indel realignment target to compare.
   * @param b Indel realignment target to compare.
   * @return True if two targets overlap.
   */
  def overlap (a: IndelRealignmentTarget, b: IndelRealignmentTarget) : Boolean = {
    ((a.getReadRange.start >= b.getReadRange.start && a.getReadRange.start <= b.getReadRange.end) ||
      (a.getReadRange.end >= b.getReadRange.start && a.getReadRange.end <= b.getReadRange.start))
  }
}

class IndelRange (indelRange: NumericRange.Inclusive[Long], readRange: NumericRange.Inclusive[Long]) {

  /**
   * Merge two identical indel ranges.
   *
   * @param ir Indel range to merge in.
   * @return Merged range.
   */
  def merge (ir: IndelRange) : IndelRange = {
    assert(indelRange == ir.getIndelRange)
    // do not need to check read range - read range must contain indel range, so if
    // indel range is the same, read ranges will overlap

    new IndelRange (indelRange,
      (readRange.start min ir.getReadRange.start) to (readRange.end max ir.getReadRange.end))
  }

  def getIndelRange (): NumericRange.Inclusive[Long] = indelRange

  def getReadRange (): NumericRange.Inclusive[Long] = readRange

}

class IndelRangeSerializer extends Serializer[IndelRange] {
  def write (kryo: Kryo, output: Output, obj: IndelRange) = {
    output.writeLong(obj.getIndelRange().start)
    output.writeLong(obj.getIndelRange().end)
    output.writeLong(obj.getReadRange().start)
    output.writeLong(obj.getReadRange().end)
  }

  def read (kryo: Kryo, input: Input, klazz: Class[IndelRange]) : IndelRange = {
    val irStart = input.readLong()
    val irEnd = input.readLong()
    val rrStart = input.readLong()
    val rrEnd = input.readLong()
    new IndelRange(
      new NumericRange.Inclusive[Long](irStart, irEnd, 1),
      new NumericRange.Inclusive[Long](rrStart, rrEnd, 1)
    )
  }
}

class SNPRange (snpSite: Long, readRange: NumericRange.Inclusive[Long]) {

  /**
   * Merge two identical SNP sites.
   *
   * @param sr SNP range to merge in.
   * @return Merged SNP range.
   */
  def merge (sr: SNPRange) : SNPRange = {
    assert(snpSite == sr.getSNPSite)
    // do not need to check read range - read range must contain snp site, so if
    // snp site is the same, read ranges will overlap

    new SNPRange(snpSite,
      (readRange.start min sr.getReadRange.start) to (readRange.end max sr.getReadRange.end))
  }

  def getSNPSite(): Long = snpSite

  def getReadRange(): NumericRange.Inclusive[Long] = readRange

}

class SNPRangeSerializer extends Serializer[SNPRange] {
  def write(kryo: Kryo, output: Output, obj: SNPRange) = {
    output.writeLong(obj.getSNPSite())
    output.writeLong(obj.getReadRange().start)
    output.writeLong(obj.getReadRange().end)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[SNPRange]): SNPRange = {
    val SNPSite = input.readLong()
    val rrStart = input.readLong()
    val rrEnd = input.readLong()
    new SNPRange(
      SNPSite,
      new NumericRange.Inclusive[Long](rrStart, rrEnd, 1)
    )
  }
}

object IndelRealignmentTarget {

  // threshold for sauing whether a pileup contains sufficient mismatch evidence
  val mismatchThreshold = 0.15

  /**
   * Generates an indel realignment target from a pileup.
   *
   * @param rod Base pileup.
   * @return Generated realignment target.
   */
  def apply(rod: Seq[ADAMPileup]): IndelRealignmentTarget = {

    /**
     * If we have a indel in a pileup position, generates an indel range.
     *
     * @param pileup Single pileup position.
     * @return Indel range.
     */
    def mapEvent(pileup: ADAMPileup): IndelRange = {
      Option(pileup.getReadBase) match {
        case None => {
          // deletion
          new IndelRange((pileup.getPosition.toLong - pileup.getRangeOffset.toLong) to (pileup.getPosition.toLong + pileup.getRangeLength.toLong - pileup.getRangeOffset.toLong),
            pileup.getReadStart.toLong to pileup.getReadEnd.toLong)
        }
        case Some(o) => {
          // insert
          new IndelRange(pileup.getPosition.toLong to pileup.getPosition.toLong,
            pileup.getReadStart.toLong to pileup.getReadEnd.toLong)
        }
      }
    }

    /**
     * If we have a point event, generates a SNPRange.
     *
     * @param pileup Pileup position with mismatch evidence.
     * @return SNP range.
     */
    def mapPoint(pileup: ADAMPileup): SNPRange = {
      val range : NumericRange.Inclusive[Long] = pileup.getReadStart.toLong to pileup.getReadEnd.toLong
      new SNPRange(pileup.getPosition, range)
    }

    // segregate into indels, matches, and mismatches
    val indels = extractIndels(rod)
    val matches = extractMatches(rod)
    val mismatches = extractMismatches(rod)

    // calculate the quality of the matches and the mismatches
    val matchQuality : Int =
      if (matches.size > 0)
        matches.map(_.getSangerQuality).reduce(_ + _)
      else
        0
    val mismatchQuality : Int =
      if (mismatches.size > 0)
        mismatches.map(_.getSangerQuality).reduce(_ + _)
      else
        0

    // check our mismatch ratio - if we have a sufficiently high ratio of mismatch quality, generate a snp event, else just generate indel events
    if (matchQuality == 0 || mismatchQuality.toDouble / matchQuality.toDouble >= mismatchThreshold) {
      new IndelRealignmentTarget(indels.map(mapEvent).toSet, mismatches.map(mapPoint).toSet)
    } else {
      new IndelRealignmentTarget(indels.map(mapEvent).toSet, HashSet[SNPRange]())
    }
  }

  def extractMismatches(rod: Seq[ADAMPileup]) : Seq[ADAMPileup] = {
    rod.filter(r => r.getRangeOffset == null && r.getNumSoftClipped == 0)
      .filter(r => r.getReadBase != r.getReferenceBase)
  }

  def extractMatches(rod: Seq[ADAMPileup]) : Seq[ADAMPileup] =
    rod.filter(r => r.getRangeOffset == null && r.getNumSoftClipped == 0)
    .filter(r => r.getReadBase == r.getReferenceBase)

  def extractIndels(rod: Seq[ADAMPileup]) : Seq[ADAMPileup] =
    rod.filter(_.getRangeOffset != null)

  /**
   * @return An empty target that has no indel nor SNP evidence.
   */
  def emptyTarget(): IndelRealignmentTarget = {
    new IndelRealignmentTarget(new HashSet[IndelRange](), new HashSet[SNPRange]())
  }
}

class IndelRealignmentTarget(indelSet: Set[IndelRange], snpSet: Set[SNPRange]) extends Logging {

  initLogging()

  // the maximum range covered by either snps or indels
  // TODO (for Frank): think about what happens when either SNP or Indel range is empty, which
  // leads to readRange being null
  lazy val readRange = (indelSet.toList.map(_.getReadRange) ++ snpSet.toList.map(_.getReadRange))
    .reduce((a: NumericRange.Inclusive[Long], b: NumericRange.Inclusive[Long]) => (a.start min b.start) to (a.end max b.end))

  /**
   * Merges two indel realignment targets.
   *
   * @param target Target to merge in.
   * @return Merged target.
   */
  def merge(target: IndelRealignmentTarget): IndelRealignmentTarget = {
    new IndelRealignmentTarget(indelSet ++ target.getIndelSet, snpSet ++ target.getSNPSet)
  }

  def isEmpty(): Boolean = {
    indelSet.isEmpty && snpSet.isEmpty
  }

  def getReadRange(): NumericRange.Inclusive[Long] = {
    if (   (snpSet != null || indelSet != null)
        && (readRange == null))
      log.warn("snpSet or indelSet non-empty but readRange empty!")
    readRange
  }

  protected[realignmenttarget] def getSNPSet(): Set[SNPRange] = snpSet

  protected[realignmenttarget] def getIndelSet(): Set[IndelRange] = indelSet

}
