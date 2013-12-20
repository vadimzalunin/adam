package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import parquet.filter.UnboundRecordFilter

class RealignIndelsSuite extends SparkFunSuite {

  def realigned_reads: Array[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("artificial.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
      .adamRealignIndels()
      .adamSortReadsByReferencePosition()
      .collect()
  }

  def gatk_realigned_reads: Array[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("artificial.realigned.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
      .collect()
  }

  sparkTest("checking realigned reads for artificial input") {
    assert(realigned_reads.size === gatk_realigned_reads.size)

    Console.println("checking relative ordering of realigned reads\"")
    val result = realigned_reads.zip(gatk_realigned_reads)
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
