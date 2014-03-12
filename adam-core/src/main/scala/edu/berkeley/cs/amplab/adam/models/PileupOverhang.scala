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
package edu.berkeley.cs.amplab.adam.models

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import edu.berkeley.cs.amplab.adam.avro.ADAMPileup
import edu.berkeley.cs.amplab.adam.serialization.AvroJavaSerializable
import scala.collection.mutable.MutableList

private[adam] object PileupOverhang {
  def apply (partition: Int,
             overhanging: TraversableOnce[Tuple2[ReferencePosition, ADAMPileup]]): PileupOverhang = {
    new PileupOverhang(partition, overhanging.toArray.map(kv => (kv._1, new AvroJavaSerializable(kv._2))))
  }
}

private[adam] class PileupOverhang (val partition: Int, 
                                    overhanging: Array[Tuple2[ReferencePosition, AvroJavaSerializable[ADAMPileup]]]) extends Serializable {
  def getPileups(): Array[Tuple2[ReferencePosition, ADAMPileup]] = {
    overhanging.map(kv => (kv._1, kv._2.avroObj))
  }

  override def toString(): String = {
    partition.toString + "\n" + overhanging.map(kv => kv._2.avroObj.toString).fold("")(_ + "\n" + _)
  }
}
