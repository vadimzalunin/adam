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

package edu.berkeley.cs.amplab.adam.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget.{RealignmentTargetFinder,IndelRealignmentTarget,TargetOrdering}
import org.apache.spark.broadcast.Broadcast
import scala.collection.immutable.TreeSet
import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions
import scala.annotation.tailrec
import scala.collection.mutable.Map
import net.sf.samtools.{Cigar, CigarOperator, CigarElement}
import scala.collection.immutable.NumericRange
import edu.berkeley.cs.amplab.adam.models.Consensus
import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.rich.RichCigar
import edu.berkeley.cs.amplab.adam.rich.RichCigar._
import edu.berkeley.cs.amplab.adam.util.MdTag

private[rdd] object RealignIndels {

  /**
   * Realigns an RDD of reads.
   *
   * @param rdd RDD of reads to realign.
   * @return RDD of realigned reads.
   */
  def apply(rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {
    new RealignIndels().realignIndels(rdd)
  }
}

private[rdd] class RealignIndels extends Serializable with Logging {
  initLogging()

  // parameter for longest indel to realign
  val maxIndelSize = 3000

  // max number of consensuses to evaluate - TODO: not currently used
  val maxConsensusNumber = 30

  // log-odds threshold for entropy improvement for accepting a realignment
  val lodThreshold = 5.0

  /**
   * Method to map a record to an indel realignment target. Returns the target to align to if the read has a
   * target and should be realigned, else returns the "empty" target.
   *
   * @param read Read to check.
   * @param targets Sorted set of realignment targets.
   * @return If overlapping target is found, returns that target. Else, returns the "empty" target.
   */
  @tailrec final def mapToTarget (read: ADAMRecord,
			          targets: TreeSet[IndelRealignmentTarget]): IndelRealignmentTarget = {
    // Perform tail call recursive binary search
    if (targets.size == 1) {
      if (TargetOrdering.equals (targets.head, read)) {
        // if there is overlap, return the overlapping target
	targets.head
      } else {
        // else, return the empty target
	IndelRealignmentTarget.emptyTarget
      }
    } else {
      // split the set and recurse
      val (head, tail) = targets.splitAt(targets.size / 2) 
      val reducedSet = if (TargetOrdering.lt(tail.head, read)) {
	head
      } else {
	tail
      }
      mapToTarget (read, reducedSet)
    }
  }

  /**
   * Given a target group with an indel realignment target and a group of reads to realign, this method
   * generates read consensuses and realigns reads if a consensus leads to a sufficient improvement.
   *
   * @param targetGroup A tuple consisting of an indel realignment target and a seq of reads
   * @return A sequence of reads which have either been realigned if there is a sufficiently good alternative
   * consensus, or not realigned if there is not a sufficiently good consensus.
   */
  def realignTargetGroup (targetGroup: (IndelRealignmentTarget, Seq[ADAMRecord])): Seq[ADAMRecord] = {
    val (target, reads)= targetGroup
    
    if (target.isEmpty) {
      // if the indel realignment target is empty, do not realign
      reads
    } else {

      var mismatchSum = 0L

      var realignedReads = List[ADAMRecord]()
      var readsToClean = List[ADAMRecord]()
      var consensus = List[Consensus]()
      
      // loop across reads and triage/generate consensus sequences
      reads.foreach(r => {
	
        // if there are two alignment blocks (sequence matches) then there is a single indel in the read
	if (r.samtoolsCigar.numAlignmentBlocks == 2) {
          // left align this indel and update the mdtag
	  r.samtoolsCigar = leftAlignIndel(r.samtoolsCigar)
	  r.mdTag.moveAlignment(r, r.samtoolsCigar)
	}

	if (r.mdTag.hasMismatches) {
          // we clean all reads that have mismatches
	  readsToClean = r :: readsToClean
	  
          // try to generate a consensus alignment - if a consensus exists, add it to our list of consensuses to test
	  consensus = Consensus.generateAlternateConsensus(r.getSequence, r.getStart, r.samtoolsCigar) match {
            case Some(o) => o :: consensus
            case None => consensus
          }
	} else {
          // if the read does not have mismatches, then we needn't process it further
	  realignedReads = r :: realignedReads
        }
      })

      if(readsToClean.length > 0 && consensus.length > 0) {

        // get reference from reads
        val (reference, refStart, refEnd) = getReferenceFromReads(reads)

        // do not check realigned reads - they must match
        val totalMismatchSumPreCleaning = readsToClean.map(sumMismatchQuality(_)).reduce(_ + _)

        /* list to log the outcome of all consensus trials. stores:  
         *  - mismatch quality of reads against new consensus sequence
         *  - the consensus sequence itself
         *  - a map containing each realigned read and it's offset into the new sequence
         */
        var consensusOutcomes = List[(Int, Consensus, Map[ADAMRecord,Int])]()

        // loop over all consensuses and evaluate
        consensus.foreach(c => {
          // generate a reference sequence from the consensus
          val consensusSequence = c.insertIntoReference(reference, refStart, refEnd)

          // evaluate all reads against the new consensus
          val sweptValues = readsToClean.map(r => {
            val (qual, pos) = sweepReadOverReferenceForQuality(r.getSequence, reference, r.qualityScores.map(_.toInt))
            val originalQual = sumMismatchQuality(r)
            
            // if the read's mismatch quality improves over the original alignment, save 
            // its alignment in the consensus sequence, else store -1 
            if (qual < originalQual) {
              (r, (qual, pos))
            } else {
              (r, (originalQual, -1))
            }
          })
          
          // sum all mismatch qualities to get the total mismatch quality for this alignment
          val totalQuality = sweptValues.map(_._2._1).reduce(_ + _)

          // package data
          var readMappings = Map[ADAMRecord,Int]()
          sweptValues.map(kv => (kv._1, kv._2._2)).foreach(m => {
            readMappings += (m._1 -> m._2)
          })

          // add to outcome list
          consensusOutcomes = (totalQuality, c, readMappings) :: consensusOutcomes
        })

        // perform reduction to pick the consensus with the lowest aggregated mismatch score
        val bestConsensusTuple = consensusOutcomes.reduce ((c1: (Int, Consensus, Map[ADAMRecord, Int]), c2: (Int, Consensus, Map[ADAMRecord, Int])) => {
          if (c1._1 <= c2._1) {
            c1
          } else {
            c2
          }
        })

        val (bestConsensusMismatchSum, bestConsensus, bestMappings) = bestConsensusTuple

        // check for a sufficient improvement in mismatch quality versus threshold
        if (totalMismatchSumPreCleaning.toDouble / bestConsensusMismatchSum > lodThreshold) {

          // if we see a sufficient improvement, realign the reads
          readsToClean.foreach(r => {

            val remapping = bestMappings(r)
            
            // if read alignment is improved by aligning against new consensus, realign
            if (remapping != -1) {
              // bump up mapping quality by 10
              r.setMapq(r.getMapq + 10)
              
              // set new start to consider offset
              r.setStart(refStart + remapping)

              // recompute cigar
              val newCigar: Cigar = if (refStart + remapping >= bestConsensus.index.head && refStart + remapping <= bestConsensus.index.end) {
                // if element overlaps with consensus indel, modify cigar with indel
                val (idElement, endLength) = if(bestConsensus.index.head == bestConsensus.index.end) {
                  (new CigarElement(bestConsensus.consensus.length, CigarOperator.I),
                   r.getSequence.length - bestConsensus.consensus.length - (bestConsensus.index.head - (refStart + remapping)))
                } else {
                  (new CigarElement((bestConsensus.index.end - bestConsensus.index.head).toInt, CigarOperator.D),
                   r.getSequence.length - (bestConsensus.index.head - (refStart + remapping)))
                }

                val cigarElements = List[CigarElement](new CigarElement((refStart + remapping - bestConsensus.index.head).toInt, CigarOperator.M),
                                                       idElement,
                                                       new CigarElement(endLength.toInt, CigarOperator.M))

                new Cigar(cigarElements)
              } else {
                // else, new cigar is all matches
                new Cigar(List[CigarElement](new CigarElement(r.getSequence.length, CigarOperator.M)))
              }

              // update mdtag and cigar
              r.mdTag.moveAlignment(r, newCigar)
              r.setCigar(newCigar.toString)
              // TODO: fix mismatchingPositions string
            }
          })
        }
      }
       
    // return all reads that we cleaned and all reads that were initially realigned
    readsToClean ::: realignedReads
    }
  }

  /**
   * From a set of reads, returns the reference sequence that they overlap.
   */
  def getReferenceFromReads (reads: Seq[ADAMRecord]): (String, Long, Long) = {
    // get reference and range from a single read
    val readRefs = reads.map((r: ADAMRecord) => {
      (r.mdTag.getReference(r), r.getStart.toLong to r.end.get)
    })
      .sortBy(_._2.head)

    // fold over sequences and append - sequence is sorted at start
    val ref = readRefs.foldRight[(String,Long)](("", readRefs.head._2.head))((refReads: (String, NumericRange[Long]), reference: (String, Long)) => {
      if (refReads._2.end < reference._2) {
        reference
      } else if (reference._2 >= refReads._2.head) {
        (reference._1 + refReads._1.substring((reference._2 - refReads._2.head).toInt), refReads._2.end)
      } else {
        // there is a gap in the sequence
        throw new IllegalArgumentException("Current sequence has a gap.")
      }
    })

    (ref._1, readRefs.head._2.head, ref._2)
  }

  /**
   * Given a cigar, returns the cigar with the position of the cigar shifted left.
   *
   * @param cigar Cigar to left align.
   * @return Cigar fully moved left.
   */
  def leftAlignIndel (cigar: Cigar): Cigar = {
    var indelPos = -1
    var pos = 0
    var indelLength = 0
    
    // find indel in cigar
    cigar.getCigarElements.map(elem => {
      elem.getOperator match {
        case (CigarOperator.I | CigarOperator.D) => {
          if(indelPos == -1) {
            indelPos = pos
            indelLength = elem.getLength
          } else {
            // if we see a second indel, return the cigar
            return cigar
          }
          pos += 1
        }
        case _ => pos += 1
      }
    })
    
    /**
     * Shifts an indel left by n. Is tail call recursive.
     * 
     * @param cigar Cigar to shift.
     * @param position Position of element to move.
     * @param shifts Number of bases to shift element.
     * @return Cigar that has been shifted as far left as possible.
     */
    @tailrec def shiftIndel (cigar: Cigar, position: Int, shifts: Int): Cigar = {
      // generate new cigar with indel shifted by one
      val newCigar = new Cigar(cigar.getCigarElements).moveLeft(position)

      // if there are no more shifts to do, or if shifting breaks the cigar, return old cigar
      if (shifts == 0 || !newCigar.isWellFormed(cigar.getLength)) {
        cigar
      } else {
        shiftIndel (newCigar, position, shifts - 1)
      }
    }

    // if there is an indel, shift it, else return
    if (indelPos != -1) {
      shiftIndel (cigar, indelPos, indelLength)
    } else {
      cigar
    }
  }

  /**
   * Sweeps the read across a reference sequence and evaluates the mismatch quality at each position. Returns
   * the alignment offset that leads to the lowest mismatch quality score. Invariant: reference sequence must be
   * longer than the read sequence.
   *
   * @param read Read to test.
   * @param reference Reference sequence to sweep across.
   * @param qualities Integer sequence of phred scaled base quality scores.
   * @return Tuple of (mismatch quality score, alignment offset).
   */
  def sweepReadOverReferenceForQuality (read: String, reference: String, qualities: Seq[Int]): (Int, Int) = {
        
    var qualityScores = List[(Int, Int)]()

    // calculate mismatch quality score for all admissable alignment offsets
    for (i <- 0 until (reference.length - read.length)) {
      qualityScores = (sumMismatchQualityIgnoreCigar(read, reference.substring(i, i + read.length), qualities), i) :: qualityScores
    }
    
    // perform reduction to get best quality offset
    qualityScores.reduce ((p1: (Int, Int), p2: (Int, Int)) => {
      if (p1._1 < p2._1) {
        p1
      } else {
        p2
      }
    })
  }

  /**
   * Sums the mismatch quality of a read against a reference. Mismatch quality is defined as the sum of the base
   * quality for all bases in the read that do not match the reference. This method ignores the cigar string, which
   * treats indels as causing mismatches.
   *
   * @param read Read to evaluate.
   * @param reference Reference sequence to look for mismatches against.
   * @param qualities Sequence of base quality scores.
   * @return Mismatch quality sum.
   */
  def sumMismatchQualityIgnoreCigar (read: String, reference: String, qualities: Seq[Int]): Int = {
    read.zip(reference)
      .zip(qualities)
      .filter(r => r._1._1 != r._1._2)
      .map(_._2)
      .reduce(_ + _)
  }

  /**
   * Given a read, sums the mismatch quality against it's current alignment position. Does NOT ignore cigar.
   *
   * @param read Read over which to sum mismatch quality.
   * @return Mismatch quality of read for current alignment.
   */
  def sumMismatchQuality (read: ADAMRecord): Int = {
    sumMismatchQuality (read.samtoolsCigar, read.mdTag, read.getStart, read.qualityScores.map(_.toInt))
  }

  /**
   * Given a cigar, mdtag, and sequence of phred scores, calculates the mismatch quality. Does NOT ignore cigar.
   *
   * @param cigar Cigar for sequence to evaluate. Gives position of inserts and deletes.
   * @param mdTag MdTag for sequence to evaluate. Gives position of mismatches.
   * @param start Start of alignment to evaluate.
   * @param phredQuals Sequence of phred scaled base quality scores.
   * @return Mismatch quality.
   */
  def sumMismatchQuality (cigar: Cigar, mdTag: MdTag, start: Long, phredQuals: Seq[Int]): Int = {
    
    var referencePos = start
    var readPos = 0
    var mismatchQual = 0

    // loop over cigar
    cigar.getCigarElements.foreach(cigarElement =>
      cigarElement.getOperator match {
	case CigarOperator.M =>
          // if mismatch, accumulate phred quality score
	  if (!mdTag.isMatch(referencePos)) {
	    mismatchQual += phredQuals (readPos)
	  }

	  readPos += 1
	  referencePos += 1
	case _ =>
	  if (cigarElement.getOperator.consumesReadBases()) {
            readPos += cigarElement.getLength
          }
          if (cigarElement.getOperator.consumesReferenceBases()) {
            referencePos += cigarElement.getLength
          }
      })

    mismatchQual
  }

  /**
   * Performs realignment for an RDD of reads. This includes target generation, read/target classification,
   * and read realignment.
   *
   * @param rdd Reads to realign.
   * @return Realigned read.
   */
  def realignIndels (rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {

    // find realignment targets
    log.info("Generating realignment targets...")
    val targets = RealignmentTargetFinder(rdd)

    // group reads by target
    log.info("Grouping reads by target...")
    val broadcastTargets = rdd.context.broadcast(targets)
    val readsMappedToTarget = rdd.groupBy (mapToTarget(_, broadcastTargets.value))

    // realign target groups
    log.info("Sorting reads by reference in ADAM RDD")
    readsMappedToTarget.flatMap(realignTargetGroup)
  }

}
