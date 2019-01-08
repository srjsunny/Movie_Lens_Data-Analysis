package KPI_2;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;



public class top20Reducer extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {
	
	
	private TreeMap<Double,String> tmap2;
	
	
	
	@Override
	public void setup(Context context)throws IOException,InterruptedException
	{
	 tmap2 = new TreeMap<Double,String>();            	
	}
	
	
	@Override
	public void reduce(DoubleWritable key,Iterable<Text>values,Context context)throws IOException,InterruptedException
	{
		//average_count    movie_name    
		
		double average_rating = key.get();
		String movie_name = null;
		
		for(Text val:values)
		{
			movie_name = val.toString().trim(); 
		}
		
		
		tmap2.put(average_rating, movie_name);
		
		if(tmap2.size()>20)
		{
			tmap2.remove(tmap2.firstKey());
		}
		
		
		
	}
	
	
	
	@Override
	public void cleanup(Context context)throws IOException,InterruptedException
	{
		for(Map.Entry<Double,String> entry : tmap2.entrySet()) 
		{
			
			  Double key = entry.getKey();              //average rating
			  String value = entry.getValue();         //movie_name
			  
			  context.write(new DoubleWritable(key), new Text(value));
			  			 //	average_rating            movie_name
		}
		
		
	}
	

}
