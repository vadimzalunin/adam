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

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import parquet.filter.UnboundRecordFilter
import edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget.RealignmentTargetFinder

class RealignIndelsSuite extends SparkFunSuite {

  lazy val mason_reads: RDD[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("small_realignment_targets.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
  }

  lazy val artificial_reads: RDD[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("artificial.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
  }

  lazy val artificial_realigned_reads: RDD[ADAMRecord] = {
    artificial_reads
      .adamRealignIndels()
      .adamSortReadsByReferencePosition()
  }

  lazy val gatk_artificial_realigned_reads: RDD[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("artificial.realigned.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
  }

  sparkTest("checking mapping to targets for artificial reads") {
    val targets = RealignmentTargetFinder(artificial_reads)
    assert(targets.size > 0)
    val broadcastTargets = artificial_realigned_reads.context.broadcast(targets)
    val readsMappedToTarget = artificial_reads.groupBy(RealignIndels.mapToTarget(_, broadcastTargets.value)).collect()

    assert(readsMappedToTarget.size > 0)

    assert(readsMappedToTarget.forall {
      case (target, reads) => reads.forall {
        read =>
          if(read.getStart <= 30)
            target.indelSet.size == 2
          else
            target.indelSet.size == 0
      }
    })
  }

  sparkTest("checking realigned reads for artificial input") {
    val artificial_realigned_reads_collected = artificial_realigned_reads.collect()
    val gatk_artificial_realigned_reads_collected = gatk_artificial_realigned_reads.collect()

    assert(artificial_realigned_reads_collected.size === gatk_artificial_realigned_reads_collected.size)

    Console.println("checking relative ordering of realigned reads")
    val result = artificial_realigned_reads_collected.zip(gatk_artificial_realigned_reads_collected)
    assert(result.forall(
      pair => pair._1.getReadName == pair._2.getReadName
    ))
    Console.println("checking start coordinate of each read")
    assert(result.forall(
      pair => pair._1.getStart == pair._2.getStart
    ))
    Console.println("checking CIGAR string of each read")
    assert(result.forall(
      pair => pair._1.getCigar == pair._2.getCigar
    ))
    Console.println("checking mapping quality of each read")
    assert(result.forall(
      pair => pair._1.getMapq == pair._2.getMapq
    ))
  }

}
