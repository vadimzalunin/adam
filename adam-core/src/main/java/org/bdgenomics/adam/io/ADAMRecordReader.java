package org.bdgenomics.adam.io;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTag;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.build.ReadFeaturesContext;
import htsjdk.samtools.cram.encoding.read_features.BaseQualityScore;
import htsjdk.samtools.cram.encoding.read_features.Deletion;
import htsjdk.samtools.cram.encoding.read_features.HardClip;
import htsjdk.samtools.cram.encoding.read_features.InsertBase;
import htsjdk.samtools.cram.encoding.read_features.Insertion;
import htsjdk.samtools.cram.encoding.read_features.Padding;
import htsjdk.samtools.cram.encoding.read_features.ReadBase;
import htsjdk.samtools.cram.encoding.read_features.RefSkip;
import htsjdk.samtools.cram.encoding.read_features.SoftClip;
import htsjdk.samtools.cram.encoding.read_features.Substitution;
import htsjdk.samtools.cram.encoding.reader.AbstractReader;
import htsjdk.samtools.cram.encoding.reader.DataReader;
import htsjdk.samtools.cram.encoding.reader.DataReaderFactory;
import htsjdk.samtools.cram.io.DefaultBitInputStream;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.CramCompressionRecord;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.cram.structure.ReadTag;
import htsjdk.samtools.cram.structure.Slice;
import htsjdk.samtools.cram.structure.SubstitutionMatrix;
import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.Log.LogLevel;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bdgenomics.formats.avro.AlignmentRecord;
import org.bdgenomics.formats.avro.Contig;

public class ADAMRecordReader extends AbstractReader {
	private SubstitutionMatrix matrix;
	private ReferenceSource rs;
	private SAMFileHeader header;
	private int alignmentStart = 0;
	private AlignmentRecord prevRecord;

	private int flags;
	private int compressionFlags;
	private int sequenceId;
	private int mateFlags;
	private int mateSequenceID;
	private int mateAlignmentStart;
	private int templateSize;
	private int recordsToNextFragment;

	private SAMSequenceRecord sequenceRecord;

	private List<AlignmentRecord> records;
	private AlignmentRecord[] prevFragments;
	private Map<String, Integer> recordOrder;
	private Contig contig;

	public ADAMRecordReader(int maxRecords) {
		super();
		records = new ArrayList<>(maxRecords);
		prevFragments = new AlignmentRecord[maxRecords];
		recordOrder = new HashMap<String, Integer>();
	}

	public static Iterator<AlignmentRecord> getRecords(Container container) throws IOException {
		List<AlignmentRecord> list = new ArrayList<>(container.nofRecords);
		ADAMRecordReader reader = new ADAMRecordReader(container.nofRecords);
		reader.sequenceId = container.sequenceId;
		reader.alignmentStart = container.alignmentStart;
		for (int i = 0; i < container.nofRecords; i++) {
			AlignmentRecord r = new AlignmentRecord();
			reader.read(r);
			list.add(r);
		}

		return list.iterator();
	}

	private void read(AlignmentRecord r) throws IOException {
		int index = records.size();
		try {
			flags = bitFlagsC.readData();
			r.setReadPaired((flags & CramCompressionRecord.MULTIFRAGMENT_FLAG) != 0);
			r.setProperPair((flags & CramCompressionRecord.PROPER_PAIR_FLAG) != 0);
			r.setReadMapped((flags & CramCompressionRecord.SEGMENT_UNMAPPED_FLAG) == 0);
			r.setMateMapped((flags & CramCompressionRecord.MATE_UNMAPPED_FLAG) == 0);
			r.setReadNegativeStrand((flags & CramCompressionRecord.NEGATIVE_STRAND_FLAG) != 0);
			r.setMateNegativeStrand((flags & CramCompressionRecord.MATE_NEG_STRAND_FLAG) != 0);
			r.setFirstOfPair((flags & CramCompressionRecord.FIRST_SEGMENT_FLAG) != 0);
			r.setSecondOfPair((flags & CramCompressionRecord.LAST_SEGMENT_FLAG) != 0);
			r.setPrimaryAlignment((flags & CramCompressionRecord.SECONDARY_ALIGNMENT_FLAG) == 0);
			r.setFailedVendorQualityChecks((flags & CramCompressionRecord.VENDOR_FILTERED_FLAG) != 0);
			r.setDuplicateRead((flags & CramCompressionRecord.DUPLICATE_FLAG) != 0);
			// not supported yet:
			// r.setSupplementaryAlignment((flags &
			// CramCompressionRecord.SUPPLEMENTARY_FLAG) != 0);

			compressionFlags = compBitFlagsC.readData();
			if (refId == -2)
				sequenceId = refIdCodec.readData();
			else
				sequenceId = refId;
			if (sequenceRecord == null || sequenceRecord.getSequenceIndex() != sequenceId) {
				sequenceRecord = header.getSequence(sequenceId);
				contig = new Contig(sequenceRecord.getSequenceName(), (long) sequenceRecord.getSequenceLength(),
						sequenceRecord.getAttribute(SAMSequenceRecord.MD5_TAG),
						sequenceRecord.getAttribute(SAMSequenceRecord.URI_TAG),
						sequenceRecord.getAttribute(SAMSequenceRecord.ASSEMBLY_TAG),
						sequenceRecord.getAttribute(SAMSequenceRecord.SPECIES_TAG));
			}
			r.setContig(contig);

			int readLength = readLengthC.readData();
			if (AP_delta)
				alignmentStart += alStartC.readData();
			else
				alignmentStart = alStartC.readData();
			r.setStart((long) alignmentStart);

			StringBuffer tagsBuffer = new StringBuffer(SAMTag.RG.name()).append(':').append(readGroupC.readData());

			if (captureReadNames)
				r.setReadName(new String(readNameC.readData(), charset));

			if ((CramCompressionRecord.DETACHED_FLAG & compressionFlags) != 0) {
				mateFlags = mbfc.readData();
				if (!captureReadNames)
					r.setReadName(new String(readNameC.readData(), charset));

				mateSequenceID = mrc.readData();
				mateAlignmentStart = malsc.readData();
				templateSize = tsc.readData();
				detachedCount++;
			} else if ((CramCompressionRecord.HAS_MATE_DOWNSTREAM_FLAG & compressionFlags) != 0) {
				recordsToNextFragment = distanceC.readData();
				prevFragments[index + recordsToNextFragment] = r;
			}

			if (r.getReadName() == null)
				r.setReadName(String.valueOf(index + 1));
			recordOrder.put(r.getReadName(), index);

			Integer tagIdList = tagIdListCodec.readData();
			byte[][] ids = tagIdDictionary[tagIdList];
			if (ids.length > 0) {
				int tagCount = ids.length;
				for (int i = 0; i < tagCount; i++) {
					tagsBuffer.appendCodePoint(ids[i][0]).appendCodePoint(ids[i][1]).append(':')
							.appendCodePoint(ids[i][2]).append('\t');
					int id = ReadTag.name3BytesToInt(ids[i]);
					DataReader<byte[]> dataReader = tagValueCodecs.get(id);
					byte[] data = dataReader.readData();
					tagsBuffer.append(new String(data));
					if (i < tagCount - 1)
						tagsBuffer.append(" ");
				}
			}
			r.setAttributes(tagsBuffer.toString());

			if ((CramCompressionRecord.SEGMENT_UNMAPPED_FLAG & flags) == 0) {
				// writing read features:
				int size = nfc.readData();
				int prevPos = 0;
				byte[] bases = new byte[readLength];
				byte[] scores = new byte[readLength];
				Arrays.fill(scores, (byte) ('?' - '!'));
				byte[] ref = sequenceId >= 0 ? rs.getReferenceBases(header.getSequence(sequenceId), true) : new byte[0];
				ReadFeaturesContext readFeaturesContext = new ReadFeaturesContext(alignmentStart, ref, bases, matrix);
				for (int i = 0; i < size; i++) {
					Byte operator = fc.readData();

					int pos = prevPos + fp.readData();
					prevPos = pos;

					switch (operator) {
					case ReadBase.operator:
						readFeaturesContext.addMismatch(pos, bc.readData());
						scores[pos] = bc.readData();
						break;
					case Substitution.operator:
						readFeaturesContext.addSubs(pos, bsc.readData());
						break;
					case Insertion.operator:
						readFeaturesContext.addInsert(pos, inc.readData());
						break;
					case SoftClip.operator:
						byte[] softClip = softClipCodec.readData();
						readFeaturesContext.addCigarElementAt(pos, CigarOperator.S, softClip.length);
						readFeaturesContext.injectBases(pos, softClip);
						break;
					case HardClip.operator:
						readFeaturesContext.addCigarElementAt(pos, CigarOperator.H, hardClipCodec.readData());
						break;
					case Padding.operator:
						readFeaturesContext.addCigarElementAt(pos, CigarOperator.P, dlc.readData());
						break;
					case Deletion.operator:
						readFeaturesContext.addCigarElementAt(pos, CigarOperator.D, dlc.readData());
						break;
					case RefSkip.operator:
						int refSkipLen = refSkipCodec.readData();
						readFeaturesContext.addCigarElementAt(pos, CigarOperator.N, refSkipLen);
						break;
					case InsertBase.operator:
						readFeaturesContext.addInsert(pos, bc.readData());
						break;
					case BaseQualityScore.operator:
						scores[pos] = qc.readData();
						break;
					default:
						throw new RuntimeException("Unknown read feature operator: " + operator);
					}
				}
				readFeaturesContext.finish();
				Cigar cigar = readFeaturesContext.createCigar();
				r.setCigar(cigar.toString());
				r.setSequence(new String(bases, Charset.forName("ascii")));
				r.setEnd((long) (alignmentStart + cigar.getReferenceLength()));

				// mapping quality:
				r.setMapq(mqc.readData());
				if ((CramCompressionRecord.FORCE_PRESERVE_QS_FLAG & compressionFlags) != 0) {
					byte[] qs = qcArray.readDataArray(readLength);
					for (int i = 0; i < qs.length; i++)
						qs[i] += '!';
					r.setQual(new String(qs, Charset.forName("ascii")));
				} else {
					for (int i = 0; i < scores.length; i++)
						scores[i] += '!';
					r.setQual(new String(scores, Charset.forName("ascii")));
				}
			} else {
				byte[] bases = new byte[readLength];
				for (int i = 0; i < bases.length; i++)
					bases[i] = bc.readData();
				r.setSequence(new String(bases, Charset.forName("ascii")));

				if ((CramCompressionRecord.FORCE_PRESERVE_QS_FLAG & compressionFlags) != 0) {
					byte[] qs = qcArray.readDataArray(readLength);
					r.setQual(new String(qs, Charset.forName("ascii")));
				}
			}

			if (prevFragments[index] != null) {
				// restore pairing information for pairs in this container:
				AlignmentRecord r1 = prevFragments[index];
				AlignmentRecord r2 = r;

				boolean properPair = isProperPair(r1, r2);
				r1.setProperPair(properPair);
				r2.setProperPair(properPair);

				r1.setMateAlignmentEnd(r2.getEnd());
				r1.setMateAlignmentStart(r2.getStart());
				r1.setMateContig(r2.getContig());
				r1.setMateMapped(r2.getReadMapped());
				r1.setMateNegativeStrand(r2.getReadNegativeStrand());

				r2.setMateAlignmentEnd(r1.getEnd());
				r2.setMateAlignmentStart(r1.getStart());
				r2.setMateContig(r1.getContig());
				r2.setMateMapped(r1.getReadMapped());
				r2.setMateNegativeStrand(r1.getReadNegativeStrand());
			}

			recordCounter++;

			prevRecord = r;
		} catch (Exception e) {
			if (prevRecord != null)
				System.err.printf("Failed at record %d. Here is the previously read record: %s\n", recordCounter,
						prevRecord.toString());
			throw new RuntimeException(e);
		}
	}

	public static class AlignmentRecordIterator implements Iterator<AlignmentRecord> {
		private SAMFileHeader header;
		private InputStream is;
		private ReferenceSource referenceSource;
		private Iterator<AlignmentRecord> it;
		private boolean eof = false;

		public AlignmentRecordIterator(SAMFileHeader header, InputStream is, ReferenceSource referenceSource) {
			this.header = header;
			this.is = is;
			this.referenceSource = referenceSource;
		}

		@Override
		public boolean hasNext() {
			if (!eof && it == null || !it.hasNext())
				try {
					it = ADAMRecordReader.readNextContainer(header, is, referenceSource);
					if (it == null) {
						eof = true;
						return false;
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}

			return it.hasNext();
		}

		@Override
		public AlignmentRecord next() {
			return it.next();
		}

		@Override
		public void remove() {
			throw new RuntimeException("Remove not implemented.");
		}

	}

	private static Iterator<AlignmentRecord> readNextContainer(SAMFileHeader header, InputStream is,
			ReferenceSource referenceSource) throws IllegalArgumentException, IllegalAccessException, IOException {
		Container container = CramIO.readContainer(is);
		if (container.isEOF())
			return null;

		List<AlignmentRecord> records = new ArrayList<>(container.nofRecords);
		for (Slice s : container.slices) {
			DataReaderFactory f = new DataReaderFactory();
			Map<Integer, InputStream> inputMap = new HashMap<Integer, InputStream>();
			for (Integer exId : s.external.keySet())
				inputMap.put(exId, new ByteArrayInputStream(s.external.get(exId).getRawContent()));

			ADAMRecordReader reader = new ADAMRecordReader(s.nofRecords);
			reader.header = header;
			reader.alignmentStart = s.alignmentStart;
			reader.rs = referenceSource;
			reader.matrix = container.h.substitutionMatrix;
			f.buildReader(reader, new DefaultBitInputStream(new ByteArrayInputStream(s.coreBlock.getRawContent())),
					inputMap, container.h, s.sequenceId);

			for (int i = 0; i < s.nofRecords; i++) {
				AlignmentRecord record = new AlignmentRecord();
				reader.read(record);
				records.add(record);
			}
		}

		return records.iterator();
	}

	public static void main(String[] args) throws IOException, IllegalArgumentException, IllegalAccessException {
		FileInputStream fis = new FileInputStream(args[0]);
		ReferenceSource referenceSource = new ReferenceSource(new File(args[1]));
		Log.setGlobalLogLevel(LogLevel.INFO);
		CramHeader cramHeader = CramIO.readCramHeader(fis);
		AlignmentRecordIterator iterator = new AlignmentRecordIterator(cramHeader.getSamFileHeader(), fis,
				referenceSource);
		while (iterator.hasNext()) {
			AlignmentRecord record = iterator.next();
			System.out.printf("@%s\n%s\n+\n%s\n", record.getReadName(), record.getSequence(), record.getQual());
		}
	}

	private static boolean isProperPair(AlignmentRecord firstEnd, AlignmentRecord secondEnd) {
		if (!firstEnd.getReadMapped() || !secondEnd.getReadMapped())
			return false;
		if (firstEnd.getContig() == null || secondEnd.getContig() == null)
			return false;

		if (!firstEnd.getContig().getContigName().equals(secondEnd.getContig().getContigName()))
			return false;

		return true;
	}
}
