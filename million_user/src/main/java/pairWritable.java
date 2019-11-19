import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class pairWritable implements WritableComparable<pairWritable> {
    private String key;
    private int val;
    public pairWritable()
    {
    }
    public pairWritable(pairWritable x){
        key=x.getKey();
        val=x.getVal();
    }
    public pairWritable(String x,int y)
    {
        key=x;
        val=y;
    }

    public String getKey(){return new String(key);}
    public int getVal(){return val;}
    public void write(DataOutput out)throws IOException{
        out.writeInt(val);
        out.writeUTF(key);
    }
    public void readFields(DataInput in)throws IOException{
        val=in.readInt();
        key=in.readUTF();
    }
    public String toString(){
        return key+" "+new String(String.valueOf(val));
    }
    public int compareTo(pairWritable p){
        return p.getVal()-val;
    }

}
