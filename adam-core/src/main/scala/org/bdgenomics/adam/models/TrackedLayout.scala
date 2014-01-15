/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.models

import org.bdgenomics.formats.avro.ADAMRecord

import scala.collection.mutable

trait TrackedLayout[T] {
  def numTracks: Int
  def trackAssignments: Map[T, Int]
}

object TrackedLayout {

  def overlaps[T](rec1: T, rec2: T)(implicit rm: ReferenceMapping[T]): Boolean = {
    val ref1 = rm.getReferenceRegion(rec1)
    val ref2 = rm.getReferenceRegion(rec2)
    ref1.overlaps(ref2)
  }

  def recordSorter(rec1: ADAMRecord, rec2: ADAMRecord): Boolean = {
    val ref1 = rec1.getContig.getContigName.toString
    val ref2 = rec2.getContig.getContigName.toString
    if (ref1.compareTo(ref2) < 0)
      true
    else if (rec1.getStart < rec2.getStart)
      true
    else if (rec1.getReadName.toString.compareTo(rec2.getReadName.toString) < 0)
      true
    else false
  }
}

class OrderedTrackedLayout[T](_reads: Traversable[T])(implicit val mapping: ReferenceMapping[T]) extends TrackedLayout[T] {

  private var trackBuilder = new mutable.ListBuffer[Track]()
  _reads.toSeq.foreach(findAndAddToTrack)
  trackBuilder = trackBuilder.filter(_.records.nonEmpty)

  val numTracks = trackBuilder.size
  val trackAssignments: Map[T, Int] =
    Map(trackBuilder.toList.zip(0 to numTracks).flatMap {
      case (track: Track, idx: Int) => track.records.map(_ -> idx)
    }: _*)

  private def findAndAddToTrack(rec: T) {
    val track: Option[Track] = trackBuilder.find(track => !track.conflicts(rec))
    track.map(_ += rec).getOrElse(addTrack(new Track(rec)))
  }

  private def addTrack(t: Track): Track = {
    trackBuilder += t
    t
  }

  private class Track(val initial: T) {

    val records = new mutable.ListBuffer[T]()
    records += initial

    def +=(rec: T): Track = {
      records += rec
      this
    }

    def conflicts(rec: T): Boolean =
      records.exists(r => TrackedLayout.overlaps(r, rec))
  }

}
