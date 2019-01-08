package KPI_1;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




public class topTenReducer extends Reducer <Text,LongWritable,LongWritable,Text> {
	
	private TreeMap<Long,String> tmap2;
	String movie_name=null;
	long count=0;
	
	
	@Override
	public void setup(Context context)throws IOException, InterruptedException
	{
	 tmap2 = new TreeMap<Long,String>();            	
	}
	
	
	@Override
	public void reduce(Text key, Iterable<LongWritable> values,Context context)throws IOException,InterruptedException
	{
		//data format => movie_name     count
		
		for(LongWritable val:values)
		{
			count = val.get();
		
		}
		
		movie_name = key.toString().trim();
		
		tmap2.put(count,movie_name);
		
		
		if(tmap2.size()>10)
		{
			tmap2.remove(tmap2.firstKey());     
		}
	
		
	}
	
	
	
	@Override
	public void cleanup(Context context) throws IOException,InterruptedException
	{
		
		for(Map.Entry<Long,String> entry : tmap2.entrySet()) 
		{
		
		     Long key = entry.getKey();              //count
			  String value = entry.getValue();      //movie_name
			  
			  context.write(new LongWritable(key),new Text(value));
		
		}
	
	
	}
	
	
	
}
