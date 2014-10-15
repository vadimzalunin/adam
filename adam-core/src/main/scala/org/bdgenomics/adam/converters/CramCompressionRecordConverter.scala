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
package org.bdgenomics.adam.converters

import htsjdk.samtools.SAMFileHeader
import htsjdk.samtools.cram.structure.{ CramCompressionRecord, ReadTag }
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }

object CramCompressionRecordConverter extends Serializable {

  def convertRecord(cramRecord: CramCompressionRecord, 
                    header: SAMFileHeader): AlignmentRecord = {
    val builder = AlignmentRecord.newBuilder()
      .setReadName(cramRecord.readName)
      .setSequence(cramRecord.readBases)
      .setQual(cramRecord.qualityScores)
      .setReadPaired(cramRecord.isMultiFragment)
      .setProperPair(cramRecord.isProperPair)
      .setReadMapped(!cramRecord.isSegmentUnmapped)
      .setMateMapped(!cramRecord.isMateUmapped)
      .setFirstOfPair(cramRecord.isFirstSegment)
      .setSecondOfPair(cramRecord.isLastSegment)
      .setFailedVendorQualityChecks(cramRecord.isVendorFiltered)
      .setDuplicateRead(cramRecord.isDuplicate)
      .setReadNegativeStrand(cramRecord.isNegativeStrand)
      .setMateNegativeStrand(cramRecord.isMateNegativeStrand)
      .setPrimaryAlignment(!cr.isSecondaryAlignment)
      .setSecondaryAlignment(cr.isSecondaryAlignment)

    // tbd:
    // - parse out attributes
    // - parse out rg info
    // - parse out program info
    // - parse out alignment end
    // - parse out contig
    // - parse out bases trimmed from start/end
    
    if (cramRecord.sequenceId != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
      builder.setStart(cramRecord.alignmentStart)
      builder.setMapq(cramRecord.mappingQuality)
    }

    builder.setMateAlignmentStart(cramRecord.mateAlignmentStart)

    // build record
    builder.build()
  }
}
