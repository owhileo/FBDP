package relation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * 求交集，对于每个record发送(record,1)，reduce
 * 时值为2才发射此record
 * @author KING
 *
 */
public class Difference {
	public static class DifferenceMap extends Mapper<LongWritable, Text, RelationA, Text>{
		@Override
		public void map(LongWritable offSet, Text line, Context context)throws
				IOException, InterruptedException{
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().toString();
			RelationA record = new RelationA(line.toString());
			context.write(record, new Text(fileName));
		}
	}
	public static class DifferenceReduce extends Reducer<RelationA, Text, RelationA, NullWritable>{
		private String target;
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			target = conf.get("target");
		}
		@Override
		public void reduce(RelationA key, Iterable<Text> value, Context context) throws
				IOException,InterruptedException{
			boolean flag = true;
			for(Text val : value){
				String temp=val.toString();
				if(!target.equals(temp.substring(temp.length()-target.length(),temp.length())))
				{
					flag=false;
					break;
				}
			}
			if(flag)
				context.write(key, NullWritable.get());
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job intersectionJob = new Job();
		intersectionJob.setJobName("differenceJob");
		intersectionJob.setJarByClass(Difference.class);

		intersectionJob.getConfiguration().set("target",args[1]);

		intersectionJob.setMapperClass(DifferenceMap.class);
		intersectionJob.setMapOutputKeyClass(RelationA.class);
		intersectionJob.setMapOutputValueClass(Text.class);
		intersectionJob.setReducerClass(DifferenceReduce.class);
		intersectionJob.setOutputKeyClass(RelationA.class);
		intersectionJob.setOutputValueClass(NullWritable.class);

		intersectionJob.setInputFormatClass(TextInputFormat.class);
		intersectionJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(intersectionJob, new Path(args[0]),new Path(args[1]));
		FileOutputFormat.setOutputPath(intersectionJob, new Path(args[2]));

		intersectionJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
