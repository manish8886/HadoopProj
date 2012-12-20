package mainapp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class StripMapper  extends MapReduceBase 
implements Mapper <NullWritable, Text, Text, MapWritable>{
	private static String StopWordFileName ="resources\\stopword.txt";
	private String stopWordRegEx;
	private void SetNextNeighbourOf(String srcString,String NeighbourName,Map<Text,MapWritable>wordNeighboursMap){

		Text wordKey = new Text(NeighbourName);//we know that it will not go past length
		Text src = new Text(srcString);
		if(!wordNeighboursMap.containsKey(src)){//So we have to create a map entry for this value
			MapWritable Neighbors = new MapWritable();//This map will be a Text and IntWritable type
			IntWritable wordCnt = new IntWritable(1);//As it occurs first time with words[i]
			Neighbors.put(wordKey,wordCnt);
			wordNeighboursMap.put(src,Neighbors);
		}else{
			MapWritable Neighbours = wordNeighboursMap.get(src);
			IntWritable wordCnt =(IntWritable) Neighbours.get(wordKey);
			if(wordCnt==null){
				wordCnt = new IntWritable(0);
				Neighbours.put(wordKey, wordCnt);
			}
			wordCnt.set(wordCnt.get()+1);
			Neighbours.put(wordKey, wordCnt);
			wordNeighboursMap.put(src, Neighbours);
		}
	}
	public void map(NullWritable key, Text value,
			OutputCollector<Text,MapWritable> output, Reporter reporter) throws IOException{
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
		Map<Text,MapWritable>wordNeighboursMap = new HashMap<Text, MapWritable>();  
		for(int i =0;i<words.length-1;i++){//As last element will not have any neighbour element
			if(	((words[i].compareTo(" ")==0)||(words[i+1].compareTo(" ")==0))||(words[i].isEmpty()||words[i+1].isEmpty())){//Not Considering spaces or Empty word for coOccurence
				continue;
			}
			SetNextNeighbourOf(words[i], words[i+1], wordNeighboursMap);
			if(!words[i].equalsIgnoreCase(words[i+1])){
				SetNextNeighbourOf(words[i+1], words[i], wordNeighboursMap);
			}
		}
		Set setNeighbours = wordNeighboursMap.entrySet();
		Iterator iter = setNeighbours.iterator();
		while(iter.hasNext()){
			Entry<Text,MapWritable>entry= (Entry<Text,MapWritable>)iter.next();
			output.collect(entry.getKey(),entry.getValue());
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