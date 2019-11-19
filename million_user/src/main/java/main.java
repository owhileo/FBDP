import com.amazonaws.services.dynamodbv2.xspec.S;
import org.apache.hadoop.fs.Path;
import org.apache.kerby.config.Conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;

public class main {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String[] arg0=new String[2];
        arg0[0]=args[0];
        arg0[1]=new String("million_temp0");
        Path path_temp0=new Path(arg0[1]);
        filter.main(arg0);
        String[] arg=new String[3];
        arg[0]=arg0[1];
        arg[1]=new String("million_temp1");
        Path path_temp=new Path(arg[1]);
        arg[2]=args[2];
        count.main(arg);
        arg[0]=arg[1];
        arg[1]=args[1];
        arg[2]=new String("10");
        topn.main(arg);

        if(fs.exists(path_temp0))
            fs.delete(path_temp0, true);
        if(fs.exists(path_temp))
            fs.delete(path_temp, true);
    }
}
