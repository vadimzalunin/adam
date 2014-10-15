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

package org.bdgenomics.adam.io;

import htsjdk.samtools.cram.index.CramIndex;
import htsjdk.samtools.cram.index.CramIndex.Entry;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.CramCompressionRecord;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.bdgenomics.adam.converters.CramCompressionRecordConverter;
import org.bdgenomics.formats.avro.AlignmentRecord;

/**
 * This class is a Hadoop reader that reads CRAM data into ADAM Records.
 *
 * @author Frank Austin Nothaft (fnothaft@berkeley.edu)
 * @date September 2014
 */
public class CRAMInputFormat extends FileInputFormat<Void,AlignmentRecord> {

    @Override InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
        // get the files to process
        FileStatus[] files = this.listStatus(conf);

        // we'll keep the indices in an array per file
        List<Entry>[] indices = new List<Entry>[files.length];
        int totalContainers = 0;

        // loop over and compute indices; increment container count
        for (int i = 0; i < files.length; i++) {
            indices[i] = readIndex(filename);
            totalContainers += indices[i].size();
        }
        
        // if we have more containers than requested splits and fewer files than
        // requested splits, we'll return the requested number
        // of splits, else, we'll return the number of containers or the number of files
        if (files.length > numSplits) {
        } else if (containerIndices.size() < numSplits) {
        } else {
        }
    }

    @Override protected boolean isSplitable(FileSystem fs, Path filename) {
        List<Entry> index = readIndex(filename);

        // we can split the file if we have more than one container
        return index.size() >= 1;
    }

    private static List<Entry> readIndex(Path filename) {
        // we look for the cram index at the position of the current file + ".crai"
        Path craiPath = new Path(filename.getParent(), filename.getChild() + ".crai");
        if (fs.exists(craiPath)) {
            InputStream craiStream = fs.open(craiPath);
            return CramIndex.readIndexFromCraiStream(craiStream);
        } else {
            // we need to read the indices from the cram file; this should be quick
            InputStream cramStream = fs.open(filename);
            return CramIndex.buidIndexForCramFile(cramStream);
        }
    }

    @Override protected FileSplit makeSplit(Path file,
                                            long start,
                                            long length,
                                            String[] hosts) {
        throw new NotImplementedException();
    }

    public static class CRAMSplit extends FileSplit {
        private List<Entry> containerIndices;

        public static CRAMSplit makeFromIndex(Path file, 
                                              List<Entry> containerIndex,
                                              int startContainer,
                                              int numContainers,
                                              JobConf conf) {
        }

        private CRAMSplit(Path file, 
                          List<Entry> indices,
                          int start,
                          int length,
                          JobConf conf) {
            super(file, start, length, conf);
            containerIndices = indices;
        }

        public List<Entry> getContainerIndices() {
            return containerIndices;
        }
    }
    
    public static class CRAMRecordReader extends RecordReader<Void,AlignmentRecord> {

        // start:  first valid data index
        private long start;
        // end:  first index value beyond the slice, i.e. slice is in range [start,end)
        private long end;
        // pos: current position in file
        private long pos;
        // file:  the file being read
        private Path file;
        // the containers indices for this split
        private List<Entry> containerIndices;

        public CRAMRecordReader(Configuration conf, CRAMSplit split) throws IOException {
            file = split.getPath();
            start = split.getStart();
            end = start + split.getLength();
            containerIndices = split.getContainerIndices();

            // open the file
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream fileIn = fs.open(file);

            // advance to the start of the stream
            stream.seek(start);
        }

        /**
         * Added to use mapreduce API.
         */
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {}

        /**
         * Added to use mapreduce API.
         */
        public Void getCurrentKey() {
            return null;
        }

        /**
         * Added to use mapreduce API.
         */
        public AlignmentRecord getCurrentValue() {
        }

        /**
         * Added to use mapreduce API.
         */
        public boolean nextKeyValue() throws IOException, InterruptedException {
        }

        /**
         * Close this RecordReader to future operations.
         */
        public void close() throws IOException {
        }

        /**
         * How much of the input has the RecordReader consumed i.e.
         */
        public float getProgress() {
        }
    }

    public RecordReader<Void, AlignmentRecord> createRecordReader(
            InputSplit genericSplit,
            TaskAttemptContext context) throws IOException, InterruptedException {
        // cast as per example in TextInputFormat
        return new CRAMRecordReader(context.getConfiguration(), 
                                    (FileSplit)genericSplit); 
    }
}
