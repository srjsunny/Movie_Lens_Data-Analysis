package KPI_1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ratingDataMapper extends Mapper<Object,Text,LongWritable,Text> {

	//data format => UserID::MovieID::Rating::Timestamp
	
		@Override
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException
		{
			
			String []tokens = value.toString().split("::");
			
			long movie_id = Long.parseLong(tokens[1]);
			
			String count = "1";
			
			context.write(new LongWritable(movie_id), new Text(count));
			              // movie_id                   count
			
			
		}
		
	
}
