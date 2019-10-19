package relation;

import java.io.IOException;

import com.amazonaws.services.dynamodbv2.xspec.S;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 获得列号为id的列上所有值为value的元组
 * @author KING
 *
 */
public class Selection {
	public static class SelectionMap extends Mapper<LongWritable, Text, RelationA, NullWritable>{
		private String id;
		private String value;
		private String opr;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			id = context.getConfiguration().get("col");
			value = context.getConfiguration().get("value");
			opr = context.getConfiguration().get("opr");
			System.out.printf("xxx:%s,%s,%s",id,value,opr);
		}
		
		@Override
		public void map(LongWritable offSet, Text line, Context context)throws 
		IOException, InterruptedException{
			RelationA record = new RelationA(line.toString());
			System.out.println(line);
			System.out.println("--------");
			if(record.isCondition(id, opr,value))
				context.write(record, NullWritable.get());
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job selectionJob = new Job();
		selectionJob.setJobName("selectionJob");
		selectionJob.setJarByClass(Selection.class);
		selectionJob.getConfiguration().set("col", args[2]);
		selectionJob.getConfiguration().set("opr", args[3]);
		selectionJob.getConfiguration().set("value", args[4]);

		selectionJob.setMapperClass(SelectionMap.class);
		selectionJob.setMapOutputKeyClass(RelationA.class);
		selectionJob.setMapOutputValueClass(NullWritable.class);

		selectionJob.setNumReduceTasks(0);

		//selectionJob.setInputFormatClass(WholeFileInputFormat.class);
		//selectionJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(selectionJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(selectionJob, new Path(args[1]));
		
		selectionJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
