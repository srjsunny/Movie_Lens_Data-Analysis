package KPI_3;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// input data from   ****userAgeOccupationGenreRatingReducer


public class averageRatingMapper extends Mapper<Object,Text,Text,Text> {
	
	//data format => age::occupation::genre             rating      (tab delimited)
	
	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
		String []tokens = value.toString().split("\t");
		
		String age_occupation_genre = tokens[0];
		
		String rating = tokens[1];
		
		String splitAgain[] = tokens[0].split("::");
		
		long age = Long.parseLong(splitAgain[0]);
		
		if(age >=18)                                 //age groups to consider => 18+ only
		{
		
		context.write(new Text(age_occupation_genre), new Text(rating));
		
		}
	
	}
	
	
}
