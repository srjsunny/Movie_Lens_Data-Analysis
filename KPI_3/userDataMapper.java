package KPI_3;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class userDataMapper extends Mapper<Object,Text,Text,Text> {

	//data from user.dat
	
   //data format => UserID::Gender::Age::Occupation::Zip-code 
	
	
	
	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
		String []tokens = value.toString().split("::");
		
		String user_id = tokens[0];
		
		String age = tokens[2];
		
		String occupation = tokens[3];
		
		
		context.write(new Text(user_id), new Text(age+"::"+occupation+"_file1"));
		             //user_id as key         // age::occupation as value
		
		
		
	}
	
	
	
	
	
	
	
	
	
}
