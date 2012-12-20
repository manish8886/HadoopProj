package mainapp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
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


public class PairMapper extends MapReduceBase 
implements Mapper <NullWritable, Text, TextPair, IntWritable>{
	private static String StopWordFileName ="resources\\stopword.txt";
	private String stopWordRegEx;
	private void SetNeighbourCount(String srcString,Map<Text,IntWritable>NeighboursCntMap){
		Text src = new Text(srcString);
		IntWritable wordCnt = NeighboursCntMap.get(src);
		if(wordCnt==null){
			wordCnt = new IntWritable(0);
			NeighboursCntMap.put(src,wordCnt);
		}
		wordCnt.set(wordCnt.get()+1);
		NeighboursCntMap.put(src, wordCnt);
	}
	public void map(NullWritable key, Text value,
			OutputCollector<TextPair,IntWritable> output, Reporter reporter) throws IOException{
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
		String[]words = paraString.split(" ");
		Map<Text,IntWritable>wordNeighboursMap = new HashMap<Text,IntWritable>();  
		for(int i =0;i<words.length-1;i++){//As last element will not have any neighbour element
			if(	((words[i].compareTo(" ")==0)||(words[i+1].compareTo(" ")==0))||(words[i].isEmpty()||words[i+1].isEmpty())){//Not Considering spaces or Empty word for coOccurence
				continue;
			}
			SetNeighbourCount(words[i],wordNeighboursMap);
			TextPair pair1 = new TextPair(words[i],words[i+1]);
			output.collect(pair1, new IntWritable(1));
			if(!words[i].equalsIgnoreCase(words[i+1])){
				SetNeighbourCount(words[i+1],wordNeighboursMap);
				TextPair pair2 = new TextPair(words[i+1],words[i]);
				output.collect(pair2, new IntWritable(1));
			}

		}
		Set setNeighbours = wordNeighboursMap.entrySet();
		Iterator iter = setNeighbours.iterator();
		while(iter.hasNext()){
			Entry<Text,IntWritable>entry= (Entry<Text,IntWritable>)iter.next();
			TextPair pair = new TextPair(entry.getKey().toString(),"*");
			output.collect(pair,new IntWritable(entry.getValue().get()));
		}
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
