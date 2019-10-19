package relation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/**
 * 表示一个关系的属性构成
 * @author KING
 *
 */
public class RelationA implements WritableComparable<RelationA>{
	private int id;
	private String name;
	private int age;
	private double weight;
	
	public RelationA(){}
	
	public RelationA(int id, String name, int age, double weight){
		this.setId(id);
		this.setName(name);
		this.setAge(age);
		this.setWeight(weight);
	}
	
	public RelationA(String line){
		String[] value = line.split(",");
		this.setId(Integer.parseInt(value[0]));
		this.setName(value[1]);
		this.setAge(Integer.parseInt(value[2]));
		this.setWeight(Double.parseDouble(value[3]));
	}
	
	public boolean isCondition(String col,String opr, String value){
		if(col.equals("id"))
			return isSatisfy(this.id,opr,Integer.parseInt(value));
		else if(col.equals("name"))
			return isSatisfy(this.name,opr,value);
		else if(col.equals("age"))
			return isSatisfy(this.age,opr,Integer.parseInt(value));
		else if(col.equals("weight"))
			return isSatisfy(this.weight,opr,Double.parseDouble(value));
		else
			return false;
	}

	boolean isSatisfy(int val,String opr,int value){
        if(opr.equals("=")||opr.equals("==")){
        	return val==value;
		}else if(opr.equals("<"))
		{
			return val<value;
		}else if(opr.equals("<="))
		{
			return val<=value;
		}else if(opr.equals(">"))
		{
			return val>value;
		}else if(opr.equals(">="))
		{
			return val>=value;
		}else if(opr.equals("!=")||opr.equals("<>"))
		{
			return val!=value;
		}else{
        	System.err.println("Unsupport operator");
        	return false;
		}
    }

	boolean isSatisfy(double val,String opr,double value){
		if(opr.equals("=")||opr.equals("==")){
			return val==value;
		}else if(opr.equals("<"))
		{
			return val<value;
		}else if(opr.equals("<="))
		{
			return val<=value;
		}else if(opr.equals(">"))
		{
			return val>value;
		}else if(opr.equals(">="))
		{
			return val>=value;
		}else if(opr.equals("!=")||opr.equals("<>"))
		{
			return val!=value;
		}else{
			System.err.println("Unsupport operator");
			return false;
		}
	}

	boolean isSatisfy(String val,String opr,String value){
		if(opr.equals("=")||opr.equals("==")){
			return val.equals(value);
		}else if(opr.equals("!=")||opr.equals("<>"))
		{
			return  !val.equals(value);
		}else{
			System.err.println("Unsupport operator");
			return false;
		}
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}
	
	public String getCol(int col){
		switch(col){
		case 0: return String.valueOf(id);
		case 1: return name;
		case 2: return String.valueOf(age); 
		case 3: return String.valueOf(weight);
		default: return null;
		}
	}

	public String getCol(String col){
		if(col.equals("id"))
		{
			return String.valueOf(id);
		}else if(col.equals("name"))
		{
			return name;
		}else if(col.equals("age"))
		{
			return String.valueOf(age);
		}else if(col.equals("weight"))
		{
			return String.valueOf(weight);
		}else
			return null;
	}
	
	public String getValueExcept(int col){
		switch(col){
		case 0: return name + "," + String.valueOf(age) + "," + String.valueOf(weight);
		case 1: return String.valueOf(id) + "," + String.valueOf(age) + "," + String.valueOf(weight);
		case 2: return String.valueOf(id) + "," + name + "," + String.valueOf(weight);
		case 3: return String.valueOf(id) + "," + name + "," + String.valueOf(age);
		default: return null;
		}
	}

	public String getValueExcept(String col){
		if(col.equals("id"))
		{
			return name + "," + String.valueOf(age) + "," + String.valueOf(weight);
		}else if(col.equals("name"))
		{
			return String.valueOf(id) + "," + String.valueOf(age) + "," + String.valueOf(weight);
		}else if(col.equals("age"))
		{
			return String.valueOf(id) + "," + name + "," + String.valueOf(weight);
		}else if(col.equals("weight"))
		{
			return String.valueOf(id) + "," + name + "," + String.valueOf(age);
		}else
			return null;
	}

	@Override
	public String toString(){
		return id + "," + name + "," + age + "," + weight;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(id);
		out.writeUTF(name);
		out.writeInt(age);
		out.writeDouble(weight);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id = in.readInt();
		name = in.readUTF();
		age = in.readInt();
		weight = in.readDouble();
	}

	@Override
	public int compareTo(RelationA o) {
		if(id == o.getId() && name.equals(o.getName()) 
				&& age == o.getAge() && weight == o.getWeight())
			return 0;
		else if(id < o.getId())
			return -1;
		else
			return 1;
	}
}
