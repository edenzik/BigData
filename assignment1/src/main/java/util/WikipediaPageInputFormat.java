/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import util.XMLInputFormat.XMLRecordReader;
//import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.*;

/**
 * To replace WikipediaPageInputFormat from cloud9 to work with hadoop 2.0
 * @author Tuan
 *
 */
public class WikipediaPageInputFormat extends IndexableFileInputFormat<LongWritable, WikipediaPage> {

	@Override
	public RecordReader<LongWritable, WikipediaPage> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		context.setStatus(split.toString());
		return new WikipediaPageRecordReader(context.getConfiguration());
	}

	/**
	 * Hadoop <code>RecordReader</code> for reading Wikipedia pages from the XML
	 * dumps.
	 */
	public static class WikipediaPageRecordReader extends RecordReader<LongWritable, WikipediaPage> {
		private XMLRecordReader reader;
		private Text text = new Text();
		private LongWritable offset = new LongWritable();
		LongWritable key;
		WikipediaPage value;

		/**
		 * Creates a <code>WikipediaPageRecordReader</code>.
		 * 
		 * @throws InterruptedException
		 */
		public WikipediaPageRecordReader(Configuration conf) throws IOException,
				InterruptedException {
			conf.set(XMLInputFormat.START_TAG_KEY, WikipediaPage.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, WikipediaPage.XML_END_TAG);
		}

		/**
		 * Creates an object for the key.
		 */
		public LongWritable createKey() {
			return new LongWritable();
		}

		/**
		 * Creates an object for the value.
		 */
		public WikipediaPage createValue() {
			return new WikipediaPage();
		}

		/**
		 * Returns the current position in the input.
		 */
		public long getPos() throws IOException {
			return reader.getPos();
		}

		/**
		 * Closes this InputSplit.
		 */
		public void close() throws IOException {
			reader.close();
		}

		/**
		 * Returns progress on how much input has been consumed.
		 */
		public float getProgress() throws IOException {
			return ((float) (reader.getPos() - reader.getStart()))
					/ ((float) (reader.getEnd() - reader.getStart()));
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public WikipediaPage getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
				InterruptedException {
			reader = new XMLRecordReader(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// Get next key value
			if (reader.nextKeyValue() == false)
				return false;
			offset = reader.getCurrentKey();
			text = reader.getCurrentValue();
			
			if (key == null) {
				key = new LongWritable();
			}
			if (value == null) {
				value = new WikipediaPage();
			}
			key.set(offset.get());
			WikipediaPage.readPage(value, text.toString());
			return true;
		}
	}

}
