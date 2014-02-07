/*
 * Copyright (c) 2014. Regents of the University of California
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

import edu.berkeley.cs.amplab.adam.avro.ADAMFastaNucleotideContig
import edu.berkeley.cs.amplab.adam.converters.FastaConverter
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.rdd.RDD
import org.kohsuke.args4j.{Option => Args4jOption, Argument}

object Fasta2Adam extends AdamCommandCompanion {
  val commandName: String = "fasta2adam"
  val commandDescription: String = "Converts a text FASTA sequence file into an ADAM FASTA file."

  def apply(cmdLine: Array[String]): AdamCommand = {
    new Fasta2Adam(Args4j[Fasta2AdamArgs](cmdLine))
  }
}

class Fasta2AdamArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "FASTA", usage = "The FASTA file to convert", index = 0)
  var fastaFile: String = null
  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM data", index = 1)
  var outputPath: String = null
}

class Fasta2Adam(protected val args: Fasta2AdamArgs) extends AdamSparkCommand[Fasta2AdamArgs] with Logging {
  val companion = Fasta2Adam

  initLogging()

  def run(sc: SparkContext, job: Job) {
    log.info("Loading FASTA data from disk.")
    val fastaData: RDD[(LongWritable, Text)] = sc.newAPIHadoopFile(args.fastaFile,
                                                                   classOf[TextInputFormat],
                                                                   classOf[LongWritable],
                                                                   classOf[Text])

    val remapData = fastaData.map(kv => (kv._1.get.toInt, kv._2.toString.toString))

    log.info("Converting FASTA to ADAM.")
    val adamFasta = FastaConverter(remapData)

    log.info("Writing records to disk.")
    adamFasta.adamSave(args.outputPath, blockSize = args.blockSize, pageSize = args.pageSize,
                       compressCodec = args.compressionCodec, disableDictionaryEncoding = args.disableDictionary)
  }
}

