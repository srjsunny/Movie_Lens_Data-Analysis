package KPI_3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ratingDataMapper extends Mapper<Object,Text,Text,Text> {

	//data from rating.dat
	
	//data format => UserID::MovieID::Rating::Timestamp
	
		@Override
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException
		{
			
			String []tokens = value.toString().split("::");
			
			String user_id = tokens[0];
			
			String movie_id = tokens[1];
			
			
			String star_rating = tokens[2];
			
			context.write(new Text(movie_id), new Text(user_id+":"+star_rating+"_ratings"));
			
			
			
		}
		
	
}
