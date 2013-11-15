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

package edu.berkeley.cs.amplab.adam.cli

import edu.berkeley.cs.amplab.adam.util._
import net.sf.samtools.{CigarOperator, TextCigarCodec}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import edu.berkeley.cs.amplab.adam.predicates.LocusPredicate
import org.kohsuke.args4j.{Option => option, Argument}
import scala.collection.immutable.StringOps
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.avro.{Base, ADAMPileup, ADAMRecord}
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions._

object LocallyRealign extends AdamCommandCompanion {
  val commandName: String = "locallyRealign"
  val commandDescription: String = "Convert an ADAM read-oriented file to an ADAM reference-oriented file"
  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton

  def apply(cmdLine: Array[String]) = {
    new LocallyRealign(Args4j[LocallyRealignArgs](cmdLine))
  }
}

class LocallyRealignArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(metaVar = "ADAMREADS", required = true, usage = "ADAM read-oriented data", index = 0)
  var readInput: String = _

  @Argument(metaVar = "DIR", required = true, usage = "Location to create reference-oriented ADAM data", index = 1)
  var output: String = _
}

class LocallyRealign(protected val args: LocallyRealignArgs) extends AdamSparkCommand[LocallyRealignArgs] {
  val companion = LocallyRealign

  def run(sc: SparkContext, job: Job) {
    val reads: RDD[ADAMRecord] = sc.adamLoad(args.readInput, Some(classOf[LocusPredicate]))

    val realignedReads = reads.adamRealignIndels ()
    
    realignedReads.adamSave(args.output)
  }

}
