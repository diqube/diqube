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
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * {@link OutputFormat} writing .diqube files which can be easily imported to diqube-server.
 * 
 * The result is the same as executing diqube-tranpose on other input files.
 *
 * @author Bastian Gloeckle
 */
public class DiqubeOutputFormat extends FileOutputFormat<NullWritable, DiqubeRow> {

  @Override
  public RecordWriter<NullWritable, DiqubeRow> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    Path destPath = getDefaultWorkFile(job, ".diqube");
    FileSystem fs = destPath.getFileSystem(job.getConfiguration());
    DataOutputStream os = fs.create(destPath, false);
    return new DiqubeRecordWriter(os);
  }

}
