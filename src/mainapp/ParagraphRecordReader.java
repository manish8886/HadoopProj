package mainapp;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;

class ParaGraphReader{
	private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private InputStream in;
	private byte[] buffer;
	// the number of bytes of real data in the buffer
	private int bufferLength = 0;
	// the current position in the buffer
	private int bufferPosn = 0;
	/**
	 * Create a line reader that reads from the given stream using the 
	 * given buffer-size.
	 * @param in
	 * @throws IOException
	 */
	ParaGraphReader(InputStream in, int bufferSize) {
		this.in = in;
		this.bufferSize = bufferSize;
		this.buffer = new byte[this.bufferSize];
	}
	public ParaGraphReader(InputStream in, Configuration conf) throws IOException {
		  this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
	}
	    
	/**
	 * Fill the buffer with more data.
	 * @return was there more data?
	 * @throws IOException
	 */
	boolean backfill() throws IOException {
		bufferPosn = 0;
		bufferLength = in.read(buffer);
		return bufferLength > 0;
	}


	/**
	 * Close the underlying stream.
	 * @throws IOException
	 */
	public void close() throws IOException {
		in.close();
	}
	/**
	 * Read from the InputStream into the given Text.
	 * @param str the object to store the given line
	 * @return the number of bytes read including the newline or carriageFeed
	 * @Assumption: We are considering that a new paragraph will always end
	 * if two continuous end char are found like "/r/n/r/n","/r/r" or "/n/n" 
	 * @throws IOException if the underlying stream throws
	 */
	public int readParaGraph(Text str) throws IOException{
		str.clear();
		boolean hadFinalNewline = false;
		boolean hadFinalReturn = false;
		boolean hitEndOfFile = false;
		boolean hitParaEnd = false;
		int startPosn = bufferPosn;
		long bytesConsumed = 0;
		outerLoop: while (true) {
			if (bufferPosn >= bufferLength) {
				if (!backfill()) {
					hitEndOfFile = true;
					break;
				}
			}
			startPosn = bufferPosn;
			for(;bufferPosn<bufferLength;++bufferPosn){
				switch(buffer[bufferPosn]){
				case '\n':
					if(!hadFinalNewline){
						hadFinalNewline = true;
					}else{
						hitParaEnd = true;
						bufferPosn++;
						break outerLoop;
					}
					break;
				case '\r':
					if(!hadFinalReturn){
						hadFinalReturn = true; 
					}else{
						if(!hadFinalNewline ){
							hitParaEnd = true;
							bufferPosn++;
							break outerLoop;
						}
					}
					break;
				default:
					hitParaEnd = false;
					hadFinalNewline = false;
					hadFinalReturn =false;
				
				}

			}
			bytesConsumed += bufferPosn - startPosn;
			int length = bufferPosn - startPosn;
			if (length >= 0) {
				str.append(buffer, startPosn, length);
			}
		}
		int newlineLength = ((hadFinalNewline ? 1 : 0) + (hadFinalReturn ? 1 : 0));
		if(hitParaEnd){
			newlineLength*=2;
		}
		if (!hitEndOfFile) {
			bytesConsumed += bufferPosn - startPosn;
			int length = bufferPosn - startPosn - newlineLength;
			if (length > 0) {
				str.append(buffer, startPosn, length);
			}
		}
		return (int)bytesConsumed;
	}
}
public class ParagraphRecordReader implements RecordReader<NullWritable, Text>{
	private long start;
	private long pos;
	private long end;
	private ParaGraphReader in;
	public ParagraphRecordReader(Configuration job, 
			FileSplit split) throws IOException {
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
//		open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		in = new ParaGraphReader(fileIn, job);
		this.pos = start;
	}
	 public NullWritable createKey() {
		    return NullWritable.get();
		  }
		  
		  public Text createValue() {
		    return new Text();
		  }

		  /** Read a line. */
		  public synchronized boolean next(NullWritable key, Text value)
		  throws IOException {
			  while (pos < end) {
				  int newSize = in.readParaGraph(value);
/*				  byte test[] = {'t','e','s','t'};
				  value.append(test, 0,4);
*/				  if (newSize == 0) {
					  return false;
				  }
				  pos += newSize;
				  return true;
			  }
			  return false;
		  }

		  /**
		   * Get the progress within the split
		   */
		  public float getProgress() {
			  if (start == end) {
				  return 0.0f;
			  } else {
				  return Math.min(1.0f, (pos - start) / (float)(end - start));
			  }
		  }
		  public  synchronized long getPos() throws IOException {
			  return pos;
		  }

		  public synchronized void close() throws IOException {
			  if (in != null) {
				  in.close(); 
			  }
		  }
		  
}
