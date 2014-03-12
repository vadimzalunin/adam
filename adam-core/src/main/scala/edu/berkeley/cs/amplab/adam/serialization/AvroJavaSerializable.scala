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
package edu.berkeley.cs.amplab.adam.serialization;

import org.apache.avro.generic.{GenericDatumWriter, GenericDatumReader, IndexedRecord}
import java.io.{ObjectOutputStream, IOException, ObjectInputStream}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecordBase, SpecificDatumReader}
import org.apache.avro.Schema
import org.apache.avro.io._

/**
 * Makes Avro objects 'Serializable', e.g.
 *
 * val avroThingy = new AvroThingy()
 * val serializableObject = new AvroSerializable[AvroThingy](avroThingy)
 * serializableObject.writeObject(...)
 * serializableObject.readObject(...)
 *
 * Works with Avro Generic and Specific objects
 *
 * @param avroObj The Avro object to be read/written
 * @tparam T The type of Avro object
 */
class AvroJavaSerializable[T <: IndexedRecord](var avroObj: T)
  extends IndexedRecord with Serializable {
  var encoder: BinaryEncoder = null
  var decoder: BinaryDecoder = null
  val writer: DatumWriter[T] = {
    avroObj match {
      case specificObj: SpecificRecordBase =>
        new SpecificDatumWriter[T](specificObj.getSchema)
      case genericObj: IndexedRecord =>
        new GenericDatumWriter[T](genericObj.getSchema)
    }
  }
  
  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    encoder = EncoderFactory.get().binaryEncoder(out, encoder)
    val className = avroObj.getClass.getCanonicalName
    // Write the full class path of the object
    encoder.writeString(className)
    // Write the object
    writer.write(avroObj, encoder)
    encoder.flush()
  }
  
  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    decoder = DecoderFactory.get().binaryDecoder(in, decoder)
    // Read the full path of the object class
    val fullClassName = decoder.readString()
    // Load the class
    val classIn = getClass.getClassLoader.loadClass(fullClassName).asInstanceOf[Class[T]]
    // Create a new instance of the class
    avroObj = classIn.newInstance()
    // Create a reader for the class
    val reader = {
      avroObj match {
        case specificObj: SpecificRecordBase =>
          new SpecificDatumReader[T](specificObj.getSchema)
        case genericObj: IndexedRecord =>
          new GenericDatumReader[T](genericObj.getSchema)
      }
    }
    // Set values on our newly created Avro object
    reader.read(avroObj, decoder)
  }
  
  def getSchema: Schema = avroObj.getSchema
  
  def put(i: Int, v: scala.Any) {
    avroObj.put(i, v)
  }
  
  def get(i: Int): AnyRef = avroObj.get(i)
}
