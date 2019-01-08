package KPI_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Driver3 {
	
	
	
	public static void main(String[] args) throws Exception {
		Path inputPath_1 = new Path(args[0]);
		Path inputPath_2 = new Path(args[1]);
		
		Path outputPath_1 = new Path(args[2]);
		Path inputPath_3   = new Path(args[3]);
		
		Path outputPath_2 = new Path(args[4]);
		
		Path finalOutput = new Path(args[5]);
		 
	
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "first job");
		
		 //set Driver class
		job.setJarByClass(Driver3.class);

	        //output format for mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

	        //output format for reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

	        //use MultipleOutputs and specify different Mapper classes and Input formats
		MultipleInputs.addInputPath(job, inputPath_1, TextInputFormat.class, movieDataMapper.class);
		MultipleInputs.addInputPath(job, inputPath_2, TextInputFormat.class, ratingDataMapper.class);
		
		
		//set Reducer class
		job.setReducerClass(dataReducer.class);

		FileOutputFormat.setOutputPath(job, outputPath_1);

		job.waitForCompletion(true);


		
		

		    Job job1 = Job.getInstance(conf, "Second job");
			
		    job1.setJarByClass(Driver3.class);
			//set Driver class

			//use multipleOutputs and specify different mapper classes
		    MultipleInputs.addInputPath(job1,outputPath_1,TextInputFormat.class,userGenreRatingMapper.class);
		    MultipleInputs.addInputPath(job1,inputPath_3,TextInputFormat.class,userDataMapper.class);
		    
		    
		    
			//output format for mapper
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);

		        
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			
			//set reducer class
			job1.setReducerClass(userAgeOccupationGenreRatingReducer.class);
			
			
			FileOutputFormat.setOutputPath(job1, outputPath_2);

			job1.waitForCompletion(true);

	
			
			
			
			
			Job job2 = Job.getInstance(conf,"Third job");
			
			job2.setJarByClass(Driver3.class);
			
			job2.setMapperClass(averageRatingMapper.class);
			job2.setReducerClass(averageRatingReducer.class);
			
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
	
	
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			
			FileInputFormat.addInputPath(job2, outputPath_2);
			FileOutputFormat.setOutputPath(job2,finalOutput);
			
			
			job2.waitForCompletion(true);
	
	}

	
	
	
	
	
	
	
	
	
	
	

}
