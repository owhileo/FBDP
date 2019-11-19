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
import java.util.ArrayList;

//import WordConcurrnce.WordConcurrenceMapper;
//import WordConcurrnce.WordConcurrenceReducer;

/**
 * 投影运算,选择列col的值输出，这里输出的值进行了剔重
 * @author KING
 *
 */
public class filter {
	public static class FilterMap extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		public void map(LongWritable offSet, Text line, Context context)throws 
		IOException, InterruptedException{
			String[] record=line.toString().split(",");
				context.write(new Text(record[0]+","+record[1]+","+record[2]+","+record[3]+","+record[4]+","+record[5]+","+record[6]+","+record[7]), new Text(line));
		}	
	}
	
	public static class FilterReduce extends Reducer<Text, Text, Text, NullWritable>{
	    @Override
		public void reduce(Text key, Iterable<Text> value, Context context) throws
		IOException,InterruptedException{

			String[] record=key.toString().split(",");
			if(!record[7].equals("2")){
                boolean flag=true;
                ArrayList<Text> x= new ArrayList<>();
                for(Text i:value)
                {
                    x.add(new Text(i));
                }
                for(int i=1;i<x.size();i++)
                {
                    if(!x.get(i-1).toString().split(",")[10].equals(x.get(i).toString().split(",")[10]))
                    {
                        flag=false;
                        break;
                    }
                }
                if(flag)
                    context.write(x.get(0), NullWritable.get());
            }
			else
			{
				for(Text i:value)
					context.write(i, NullWritable.get());
			}
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job filterJob = new Job();
		filterJob.setJobName("filterJob");
		filterJob.setJarByClass(filter.class);

		filterJob.setMapperClass(FilterMap.class);
		filterJob.setMapOutputKeyClass(Text.class);
		filterJob.setMapOutputValueClass(Text.class);

        //filterJob.setNumReduceTasks(3);

		filterJob.setReducerClass(FilterReduce.class);
		filterJob.setOutputKeyClass(Text.class);
		filterJob.setOutputValueClass(NullWritable.class);

		filterJob.setInputFormatClass(TextInputFormat.class);
		filterJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(filterJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(filterJob, new Path(args[1]));
		
		filterJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
