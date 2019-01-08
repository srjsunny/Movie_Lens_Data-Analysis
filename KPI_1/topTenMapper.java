package KPI_1;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class topTenMapper extends Mapper<Object,Text,Text,LongWritable> {

	private TreeMap<Long,String> tmap;
	String movie_name=null;
	long count=0;
	
	@Override
	public void setup(Context context)throws IOException, InterruptedException
	{
	 tmap = new TreeMap<Long,String>();            	
	}
	
	
	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
		//data format => movie_name   count    (tab delimited)  from dataReducer 
		
		String []tokens =  value.toString().split("\t");
		
		count = Long.parseLong(tokens[1]);
		
		movie_name = tokens[0].trim();
		
		tmap.put(count, movie_name);
		
		if(tmap.size() >10)                    //if size crosses 10 we will remove the topmost key-value pair.
		{ 
			tmap.remove(tmap.firstKey());    
		}
		
		
		
	}
	
	
	@Override
	public void cleanup(Context context) throws IOException,InterruptedException
	{
		
		for(Map.Entry<Long,String> entry : tmap.entrySet()) {
		
			Long key = entry.getKey();              //count
			  String value = entry.getValue();      //movie_name
			  
			  context.write(new Text(value),new LongWritable(key));
		
		}
		
		
		
	}
	
	
	
	
	
	

}
