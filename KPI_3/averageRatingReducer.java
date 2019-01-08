package KPI_3;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class averageRatingReducer extends Reducer <Text,Text,Text,Text>{
	
	@Override
	public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException
	{
		//key                  value (ratings)
	//age::occupation::genre    [ 1, 4 ,2,3,5,5,5 .......]
	
	//one user watching multiple movies
      
		double avg = 0.0;
        double sum = 0.0;
        long count = 0;
		
		for(Text val:values)
		{
			String temp = val.toString();
		    long rating = Long.parseLong(temp);
			
		    sum+=rating; 
			count++;
			
			
		}
		
		avg = sum/count;
		
		String average_rating = String.valueOf(avg);
		
		context.write(new Text(average_rating), new Text(key));
		
		
		
		
		
		
	}
	

}
