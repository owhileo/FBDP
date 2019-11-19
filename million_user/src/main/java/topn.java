import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//import WordConcurrnce.WordConcurrenceMapper;
//import WordConcurrnce.WordConcurrenceReducer;

/**
 * 投影运算,选择列col的值输出，这里输出的值进行了剔重
 * @author KING
 *
 */
public class topn {
	public static class TopNMap extends Mapper<Text, Text , Text,pairWritable>{
		
		@Override
		public void map(Text key, Text value, Context context)throws
		IOException, InterruptedException{
			String[] record=key.toString().split(",");
			context.write(new Text(record[1]),new pairWritable(record[0]+"\t"+record[1],Integer.parseInt(value.toString())));
		}	
	}
	
	public static class TopNReduce extends Reducer< Text, pairWritable , Text, LongWritable>{
        private int n;
        @Override
        protected void setup(Context context)
        {
            n=Integer.parseInt(context.getConfiguration().get("n"));
        }
		@Override
		public void reduce(Text key, Iterable<pairWritable> value, Context context) throws
		IOException,InterruptedException{
			ArrayList<pairWritable> x=new ArrayList<pairWritable>();
        	for(pairWritable i:value)
        		x.add(new pairWritable(i));
        	x.sort(pairWritable::compareTo);
            for(int i=0;i<n;i++)
            {
                context.write(new Text(x.get(i).getKey()),new LongWritable(x.get(i).getVal()));
            }
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job topnJob = new Job();
		topnJob.setJobName("topnJob");
		topnJob.setJarByClass(topn.class);
		topnJob.getConfiguration().setInt("n", Integer.parseInt(args[2]));
		
		topnJob.setMapperClass(TopNMap.class);
		topnJob.setMapOutputKeyClass(Text.class);
		topnJob.setMapOutputValueClass(pairWritable.class);


		topnJob.setReducerClass(TopNReduce.class);
		topnJob.setOutputKeyClass(Text.class);
		topnJob.setOutputValueClass(LongWritable.class);

		topnJob.setInputFormatClass(KeyValueTextInputFormat.class);
		topnJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(topnJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(topnJob, new Path(args[1]));
		
		topnJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
