/**
 * diqube: Distributed Query Base.
 *
 * Copyright (C) 2015 Bastian Gloeckle
 *
 * This file is part of diqube.
 *
 * diqube is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.diqube.hadoop;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link OutputFormat} writing .diqube files which can be easily imported to diqube-server.
 * 
 * The result is the same as executing diqube-tool "transpose" on other input files.
 *
 * @author Bastian Gloeckle
 */
public class DiqubeOutputFormat extends FileOutputFormat<NullWritable, DiqubeRow> {
  private static final Logger logger = LoggerFactory.getLogger(DiqubeOutputFormat.class);

  private static final String PROP_FILE_COMMENT = DiqubeOutputFormat.class.getName() + ".fileComment";
  private static final String PROP_MEMORY_CHECK_ROW_COUNT = DiqubeOutputFormat.class.getName() + ".memoryCheckRowCount";
  private static final Long DEFAULT_MEMORY_CHECK_ROW_COUNT = 1000L;
  private static final String PROP_MEMORY_FLUSH_MB = DiqubeOutputFormat.class.getName() + ".memoryFlushMb";
  private static final Long DEFAULT_MEMORY_FLUSH_MB = 3 * 1024L; // 3 GB

  @Override
  public RecordWriter<NullWritable, DiqubeRow> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    Path destPath = getDefaultWorkFile(job, ".diqube");
    logger.info("Will write .diqube file to {}", destPath.toString());
    FileSystem fs = destPath.getFileSystem(job.getConfiguration());
    DataOutputStream os = fs.create(destPath, false);
    return new DiqubeRecordWriter(os, //
        job.getConfiguration().get(PROP_FILE_COMMENT, ""), //
        Long.valueOf(
            job.getConfiguration().get(PROP_MEMORY_CHECK_ROW_COUNT, DEFAULT_MEMORY_CHECK_ROW_COUNT.toString())), //
        Long.valueOf(job.getConfiguration().get(PROP_MEMORY_FLUSH_MB, DEFAULT_MEMORY_FLUSH_MB.toString())) //
    );
  }

  /**
   * Set the "comment" that should be written to the output file.
   */
  public static void setFileComment(Job job, String fileComment) {
    job.getConfiguration().set(PROP_FILE_COMMENT, fileComment);
  }

  /**
   * Set number of rows after which to trigger a check for the consumed memory and if we should flush to disk.
   */
  public static void setMemoryCheckRowCount(Job job, long memoryCheckRowCount) {
    job.getConfiguration().set(PROP_MEMORY_CHECK_ROW_COUNT, Long.toString(memoryCheckRowCount));
  }

  /**
   * Set memory consumption (in MB) after which we should compress the current values, create a new TableShard and flush
   * that into the output file.
   * 
   * <p>
   * The memory consumption is an approximation of the memory that the preliminary data structures use, to which input
   * data is added when a new {@link DiqubeRow} is passed to the {@link DiqubeRecordWriter}. It is (1) not exact, (2)
   * does not match the size the Reducer input might be and (3) does not at all match the data size after all
   * compression etc is applied. You might want to choose a value as low as half the amount available to each reducer,
   * but you probably want to set it as high as possible. If this value is too low, you might end up with a high number
   * of TableShards and only a few rows per TableShard (in the worst case even 1 row per table shard, which is
   * definitely not good!).
   * 
   * <p>
   * Each output file can handle multiple TableShards. The less TableShards are created, the better the compression, the
   * worse the parallelism in diqube-servers when executing a query. The creation of TableShards takes up quite some
   * memory, therefore this setting can be used to not exceed the memory available on the reducers.
   */
  public static void setMemoryFlushMb(Job job, long memoryFlushMb) {
    job.getConfiguration().set(PROP_MEMORY_FLUSH_MB, Long.toString(memoryFlushMb));
  }
}
