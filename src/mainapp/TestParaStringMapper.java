package mainapp;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TestParaStringMapper extends MapReduceBase 
implements Mapper <NullWritable, Text, Text,Text> {
	private static String StopWordFileName ="resources\\stopword.txt";
	private String stopWordRegEx;

	public void map(NullWritable key, Text value,
			OutputCollector<Text,Text> output, Reporter reporter) throws IOException{
		String paraString = value.toString();
		paraString=paraString.replaceAll("^\\s+","");
		paraString=paraString.replaceAll("\r\n", " ");
		paraString = paraString.replaceAll("[^a-zA-z0-9\\s]","" );
		paraString = paraString.trim().replaceAll(" +"," ");
		if(!stopWordRegEx.isEmpty()){
			Pattern stopWords = Pattern.compile(stopWordRegEx, Pattern.CASE_INSENSITIVE);
			Matcher matcher = stopWords.matcher(paraString);
			paraString = matcher.replaceAll("");   
		}
		
		
		
/*		String[]words = paraString.split(" ");
		for(int i =0;i<words.length;i++){
			mapperKey.set(words[i]);
			output.collect(mapperKey,new IntWritable(1));
		}*/
		Text outVal = new Text("0000");
		Text mapperKey= new Text(paraString);
		output.collect(outVal,mapperKey);
	}
	@Override
	public void configure(JobConf job){
		try{
			stopWordRegEx = new String();
			FileSystem fs = FileSystem.get(job);
			Path filePath = new Path(fs.getWorkingDirectory(),StopWordFileName);
			if(!fs.exists(filePath)){
				System.out.println("Stop word File is not present");
				return;
			}
			FileStatus status=fs.getFileStatus(filePath);
			byte[] bytes = new byte[(int)status.getLen()];
			FSDataInputStream inStream= fs.open(filePath);
			inStream.readFully(bytes);
			String stopWords = new String(bytes);
			String[]stopWordsArray = stopWords.split("\r\n");
			int ArrayLen = stopWordsArray.length;
			for(int i=0;i<ArrayLen;i++){
				if(i==0){
					stopWordRegEx = "\\b(?:"+(stopWordsArray[i])+"|";
				}
				else if(i!=ArrayLen-1){
					stopWordRegEx = stopWordRegEx+(stopWordsArray[i])+"|";
				}else{
					stopWordRegEx=stopWordRegEx+(stopWordsArray[i])+")\\b\\s*";
				}
			}
			System.out.println(stopWordRegEx);
		}catch(IOException E){
			System.out.println("Error in configure function in Stripe Mapper Function");
		}
	}
}

