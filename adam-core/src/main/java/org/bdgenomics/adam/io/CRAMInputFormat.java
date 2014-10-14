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
import org.bdgenomics.formats.avro.AlignmentRecord;

/**
 * This class is a Hadoop reader that reads CRAM data into ADAM Records.
 *
 * @author Frank Austin Nothaft (fnothaft@berkeley.edu)
 * @date September 2014
 */
public class CRAMInputFormat extends FileInputFormat<Void,AlignmentRecord> {
    
    public static class CRAMRecordReader extends RecordReader<Void,AlignmentRecord> {

        public CRAMRecordReader(Configuration conf, FileSplit split) throws IOException {
        }

        /**
         * Position the input stream at the start of the first record.
         */
        private void positionAtFirstRecord(FSDataInputStream stream) throws IOException {
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
         * Create an object of the appropriate type to be used as a key.
         */
        public AlignmentRecord createKey() {
        }

        /**
         * Create an object of the appropriate type to be used as a value.
         */
        public AlignmentRecord createValue() {
        }

        /**
         * Returns the current position in the input.
         */
        public long getPos() { 
        }

        /**
         * How much of the input has the RecordReader consumed i.e.
         */
        public float getProgress() {
        }

        public String makePositionMessage() {
        }

        /**
         * Reads the next key/value pair from the input for processing.
         */
        public boolean next(Text value) throws IOException {
        }


        private int appendLineInto(Text dest, boolean eofOk) throws EOFException, IOException {
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
