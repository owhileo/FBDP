package relation;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
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


/**
 * 自然连接操作,在属性col上进行连接
 *
 * @author KING
 */
public class NaturalJoin {
    public static class NaturalJoinMap extends Mapper<LongWritable, Text, Text, Text> {
        private String col;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            col = context.getConfiguration().get("col");
        }

        @Override
        public void map(LongWritable offSet, Text line, Context context) throws
                IOException, InterruptedException {
            int num = line.toString().split(",").length;
            if (num == 4) {
                RelationA record = new RelationA(line.toString());
                context.write(new Text(record.getCol(col)), new Text("a " + record.getValueExcept(col)));
            } else if (num == 3) {
                RelationB record = new RelationB(line.toString());
                context.write(new Text(record.getCol(col)), new Text("b " + record.getValueExcept(col)));
            }

        }
    }

    public static class NaturalJoinReduce extends Reducer<Text, Text, Text, NullWritable> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws
                IOException, InterruptedException {
            ArrayList<Text> setR = new ArrayList<Text>();
            ArrayList<Text> setS = new ArrayList<Text>();
            //按照来源分为两组然后做笛卡尔乘积
            for (Text val : value) {
                String[] recordInfo = val.toString().split(" ");
                if (recordInfo[0].equals("a"))
                    setR.add(new Text(recordInfo[1]));
                else
                    setS.add(new Text(recordInfo[1]));
            }
            for (int i = 0; i < setR.size(); i++) {
                for (int j = 0; j < setS.size(); j++) {
                    String[] a = setR.get(i).toString().split(",");
                    String[] b = setS.get(i).toString().split(",");
					Text t = new Text(key.toString() + "," + a[0] + "," + a[1] + "," + b[0] + "," + a[2] + "," + b[1]);
                    context.write(t, NullWritable.get());
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job naturalJoinJob = new Job();
        naturalJoinJob.setJobName("naturalJoinJob");
        naturalJoinJob.setJarByClass(NaturalJoin.class);
        naturalJoinJob.getConfiguration().set("col", args[3]);

        naturalJoinJob.setMapperClass(NaturalJoinMap.class);
        naturalJoinJob.setMapOutputKeyClass(Text.class);
        naturalJoinJob.setMapOutputValueClass(Text.class);

        naturalJoinJob.setReducerClass(NaturalJoinReduce.class);
        naturalJoinJob.setOutputKeyClass(Text.class);
        naturalJoinJob.setOutputValueClass(NullWritable.class);

        naturalJoinJob.setInputFormatClass(TextInputFormat.class);
        naturalJoinJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(naturalJoinJob, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(naturalJoinJob, new Path(args[2]));

        naturalJoinJob.waitForCompletion(true);
        System.out.println("finished!");
    }
}
