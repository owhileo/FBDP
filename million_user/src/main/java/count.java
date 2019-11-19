import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

//import WordConcurrnce.WordConcurrenceMapper;
//import WordConcurrnce.WordConcurrenceReducer;

/**
 * 投影运算,选择列col的值输出，这里输出的值进行了剔重
 * @author KING
 *
 */
public class count {
	public static class CountMap extends Mapper<LongWritable, Text, Text, LongWritable>{
		private String[] select_list;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			select_list = context.getConfiguration().get("list").split(",");
		}
		
		@Override
		public void map(LongWritable offSet, Text line, Context context)throws 
		IOException, InterruptedException{
			String[] record=line.toString().split(",");
			boolean flag=false;
			for(String i:select_list)
			{
				if(record[7].equals(i))
				{
					flag=true;
					break;
				}
			}
			if(flag)
				context.write(new Text(record[1]+","+record[10]), new LongWritable(1));
		}	
	}
	
	public static class CountReduce extends Reducer<Text, LongWritable, Text, LongWritable>{
	    @Override
		public void reduce(Text key, Iterable<LongWritable> value, Context context) throws
		IOException,InterruptedException{
			long sum=0;
			for(LongWritable i:value)
			{
				sum++;
			}
			context.write(key, new LongWritable(sum));
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job countJob = new Job();
		countJob.setJobName("countJob");
		countJob.setJarByClass(count.class);
		countJob.getConfiguration().set("list", args[2]);
		
		countJob.setMapperClass(CountMap.class);
		countJob.setMapOutputKeyClass(Text.class);
		countJob.setMapOutputValueClass(LongWritable.class);

        //countJob.setNumReduceTasks(3);

		countJob.setReducerClass(CountReduce.class);
		countJob.setOutputKeyClass(Text.class);
		countJob.setOutputValueClass(LongWritable.class);

		countJob.setInputFormatClass(TextInputFormat.class);
		countJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(countJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(countJob, new Path(args[1]));
		
		countJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
