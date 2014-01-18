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
package edu.berkeley.cs.amplab.adam.rich

import org.scalatest.FunSuite
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import RichADAMRecord._
import net.sf.samtools.Cigar
import RichCigar._

class RichCigarSuite extends FunSuite {

  test("moving 2 bp from a deletion to a match operator") {
    val read = ADAMRecord.newBuilder().setReadMapped(true).setStart(0).setCigar("10M10D10M").build()
    val newCigar = new Cigar(read.samtoolsCigar.getCigarElements).moveLeft(1)
    val newCigar2 = new Cigar(newCigar.getCigarElements).moveLeft(1)
    assert(newCigar2.getReadLength == read.samtoolsCigar.getReadLength + 2)
  }

  test("moving 2 bp from a insertion to a match operator") {
    val read = ADAMRecord.newBuilder().setReadMapped(true).setStart(0).setCigar("10M10I10M").build()
    val newCigar = new Cigar(read.samtoolsCigar.getCigarElements).moveLeft(1)
    val newCigar2 = new Cigar(newCigar.getCigarElements).moveLeft(1)
    assert(newCigar2.getReadLength == read.samtoolsCigar.getReadLength)
  }

}
