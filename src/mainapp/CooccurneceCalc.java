package mainapp;

import java.util.Date;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CooccurneceCalc extends Configured
implements Tool{

	private static long inputSize =-1;
	private int CreateDirInHDFS(String dirName, JobConf conf){
		try{
			FileSystem fileSystem = FileSystem.get(conf);
			Path path = new Path(fileSystem.getHomeDirectory(),dirName);
			if (fileSystem.exists(path)) {
				System.out.println("Dir " + dirName + " already not exists");
				return 0;
			}
			fileSystem.mkdirs(path);
			return 1;
		}catch(IOException E){
			System.out.println("Exception while creating Dir" + dirName );
			return -1;
		}
	}
	private int CopyFromLocalDirToHDFSDir(String srcName, String destName,
			JobConf conf) {
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			Path hdfsPath=null;
			if(!destName.isEmpty()){
				hdfsPath = new Path(fileSystem.getHomeDirectory(),destName);
			}else{
				hdfsPath = fileSystem.getHomeDirectory();
			}
			fileSystem.copyFromLocalFile(new Path(srcName),hdfsPath);
			return 1;
		} catch (IOException E) {
			System.out.println("Exception while Copying From Local Disk"+ srcName);
			return -1;
		}
	}
	private int CopyToLocalDir(String srcName, String destName,
			JobConf conf) {
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			Path hdfsPath = new Path(fileSystem.getHomeDirectory(),srcName);
			File file = new File(destName);
			fileSystem.copyToLocalFile(hdfsPath,new Path(destName));
			return 1;
		} catch (IOException E) {
			System.out.println("Exception while copying to local dir"+destName);
			return -1;
		}
	}
	private int DeleteDirOnHDFS(String srcLocalName,JobConf conf){
		try{
			FileSystem fs = FileSystem.get(conf);
			Path localDirPath = new Path(fs.getHomeDirectory(),srcLocalName);
			if(!fs.exists(localDirPath)){
				return 1; //file not present
			}
			fs.delete(localDirPath, true);
			return 1;
		}catch(IOException E){
			System.out.println("Exception while deleting "+srcLocalName);
			return -1;
		}
	}
	private void MarkFileForDelete(String[] srcLocalName,JobConf conf){
		try{
			FileSystem fs = FileSystem.get(conf);
			for(int i=0;i<srcLocalName.length;i++){
				Path localDirPath = new Path(fs.getHomeDirectory(),srcLocalName[i]);
				if(!fs.exists(localDirPath)){
					continue; //file not present
				}
				fs.deleteOnExit(localDirPath);
			}
		}catch(IOException E){
			System.out.println("Exception while marking for delete");
		}
	}

	private void SetParamForStripApproach(JobConf conf){
		//SetInputFormat
		conf.setInputFormat(TextParaInputFormat.class);
		// specify a mapper
		conf.setMapperClass(StripMapper.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(MapWritable.class);

		conf.setNumReduceTasks(10);
		// specify a reducer
		conf.setReducerClass(StripReducer.class);

		conf.setOutputKeyClass(TextPair.class);
		conf.setOutputValueClass(DoubleWritable.class);

		/*		// specify input and output dirs
		FileInputFormat.addInputPath(conf, new Path("input"));
		FileOutputFormat.setOutputPath(conf,new Path("output"));*/
	}

	private void SetParamForPairApproach(JobConf conf){
		//SetInputFormat
		conf.setInputFormat(TextParaInputFormat.class);
		// specify a mapper
		conf.setMapperClass(PairMapper.class);

		conf.setMapOutputKeyClass(TextPair.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setNumReduceTasks(10);
		// specify a reducer
		conf.setReducerClass(PairReducer.class);

		conf.setOutputKeyClass(TextPair.class);
		conf.setOutputValueClass(DoubleWritable.class);

		/*		// specify input and output dirs
		FileInputFormat.addInputPath(conf, new Path("input"));
		FileOutputFormat.setOutputPath(conf,new Path("output"));
		 */	}
	private void SetParamForTest(JobConf conf){
		//SetInputFormat
		conf.setInputFormat(TextParaInputFormat.class);
		// specify a mapper
		conf.setMapperClass(TestParaStringMapper.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		/*
		    conf.setNumReduceTasks(5);*/
		// specify a reducer
		conf.setReducerClass(TestParaInputReducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

	}
	private void SetParamForTestSortApproach(JobConf conf){
		//SetInputFormat
		conf.setInputFormat(TextParaInputFormat.class);
		// specify a mapper
		conf.setMapperClass(PairMapper.class);

		conf.setMapOutputKeyClass(TextPair.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setNumReduceTasks(1);
		// specify a reducer
		conf.setReducerClass(TestSortingReducer.class);

		conf.setOutputKeyClass(TextPair.class);
		conf.setOutputValueClass(IntWritable.class);

	}	
	public int run(String[] args)throws Exception{
		JobClient client = new JobClient();
		JobConf conf = new JobConf(getConf(),getClass());
		conf.setJobName("CoOccurance");
		if(args[0].isEmpty()){
			return -1;
		}
		String inputFullPath = args[0];
		String resourceFullPath = args[1];
		String[] inputTokens = args[0].split("\\\\");
		String	inputDirName= inputTokens[inputTokens.length-1];

		if(CopyFromLocalDirToHDFSDir(inputFullPath,"", conf)==-1){//The second string is empty as we will mount the whole input dir to hdfs home
			System.out.println(" Input dir copy operation Failed");
			return -1;
		}

		String ResourceDirName="";
		if(!args[1].isEmpty()){
			inputTokens = args[1].split("\\\\");
			ResourceDirName= inputTokens[inputTokens.length-1];
			if(CopyFromLocalDirToHDFSDir(resourceFullPath,"",conf)==-1){
				System.out.println(" Resource dir copy operation Failed");
				return -1;
			}
		}
		// specify input and output dirs
		FileInputFormat.addInputPath(conf, new Path(inputDirName));
		FileOutputFormat.setOutputPath(conf,new Path("output"));//Output Path is default to "output"

		//And mark these file for delete
		if(!ResourceDirName.isEmpty())
		{	
			String dir[] = {inputDirName,ResourceDirName};//HDFS names
			MarkFileForDelete(dir, conf);
		}else{
			String dir[] = {inputDirName};//HDFS names
			MarkFileForDelete(dir, conf);

		}

		FileSystem fs = FileSystem.get(conf);
		Path inputHdfs = new Path(fs.getWorkingDirectory(),inputDirName);
		if(fs.exists(inputHdfs)){
			FileStatus[] statusList = fs.listStatus(inputHdfs);
			inputSize=0;
			for(int i =0;i<statusList.length;i++){
				if(!statusList[i].isDir()){
				inputSize+=statusList[i].getLen();
				}
			}
		}
		
		if(args[3].equalsIgnoreCase("0")){//Strip Approach
			SetParamForStripApproach(conf);
		}else{//Pair
			SetParamForPairApproach(conf);
		}
		client.setConf(conf);
		JobClient.runJob(conf);
		//<TODO:Uncomment>
		if(!args[2].isEmpty()){
			CopyToLocalDir("output\\", args[2], conf);
		}
		DeleteDirOnHDFS("output", conf);
		return 0;
	}
	private static void PrepareLogFile(String path,long timeTaken){
		File file = new File(path);
		try
		{
			if(!file.exists()){
				file.createNewFile();
			}
			FileWriter filewriter = new FileWriter(file.getAbsoluteFile(),true);
			BufferedWriter writer = new BufferedWriter(filewriter);
			Long timeSpent = new Long(timeTaken);
			Long size = new Long(inputSize);
			String content = timeSpent.toString()+","+size.toString();
			writer.write(content);
			writer.newLine();
			writer.close();

		}catch (IOException e) {
			e.printStackTrace();
		}		
	}
	public static void main(String[] args) {
		try{
			BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
			String [] newArgs = new String[4];
			System.out.println("Enter Full Path of Input Dir:");
			newArgs[0] = console.readLine();
			System.out.println("Enter Full Path of Resources Dir:");
			newArgs[1] = console.readLine();
			System.out.println("Enter Full Path of OutPutDir:");
			newArgs[2] = console.readLine();
			System.out.println("Press 0 for Strip Aprroach and 1 for pair approach");
			newArgs[3] = console.readLine();
			
			long start = new Date().getTime();//Start Calculating the time
			int exitCode = ToolRunner.run(new CooccurneceCalc(),newArgs);
			long end = new Date().getTime();
			System.out.println("Job took "+(end-start) + "milliseconds");
			String outDirPath = newArgs[2];
			if(outDirPath.substring(outDirPath.length()-1).equalsIgnoreCase(File.separator)){
				outDirPath = outDirPath+"log"; 
			}else{
				outDirPath = outDirPath+File.separator+"log";
			}
			PrepareLogFile(outDirPath,end-start);
		}catch(Exception E){
			E.printStackTrace();
		}

	}

}
