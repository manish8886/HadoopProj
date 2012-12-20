package mainapp;
import java.io.IOException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class TextParaInputFormat extends FileInputFormat<NullWritable, Text>{
@Override
protected boolean isSplitable(FileSystem fs, Path file) {
    return false;
  }

  public RecordReader<NullWritable, Text> getRecordReader(
                                          InputSplit genericSplit, JobConf job,
                                          Reporter reporter)
    throws IOException {
    reporter.setStatus(genericSplit.toString());
    return new ParagraphRecordReader(job, (FileSplit) genericSplit);
  }
}
