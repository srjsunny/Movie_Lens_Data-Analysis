package KPI_2;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class top20Mapper extends Mapper<Object,Text,DoubleWritable,Text>{
	

	private TreeMap<Double,String> tmap;
	String movie_name = null;
	double average_rating;
	
	
	@Override
	public void setup(Context context)throws IOException,InterruptedException
	{
		tmap = new TreeMap<Double,String>();
	}
	
	
	
	
	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
		//data format => movie_name   average_rating     (tab delimited)
		
		
		 String []tokens = value.toString().split("\t");
		
		 movie_name = tokens[0];
		 average_rating = Double.parseDouble(tokens[1]);
		
		
		 tmap.put(average_rating, movie_name);
		 
		 if(tmap.size()>20)
		 {
			 tmap.remove(tmap.firstKey());
		 }
		 
		 	
		
	}
	
	
	
	@Override
	public void cleanup(Context context)throws IOException,InterruptedException
	{
		for(Map.Entry<Double,String> entry : tmap.entrySet()) 
		{
			
			  Double key = entry.getKey();              //average rating
			  String value = entry.getValue();         //movie_name
			  
			  context.write(new DoubleWritable(key), new Text(value));
			  			 //	average_rating            movie_name
		}
		
		
	}
	
	

}
