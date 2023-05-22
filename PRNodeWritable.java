import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.*;

public class PRNodeWritable implements Writable {


    private int page;
    private double PRval;
	private ArrayList<Integer> connectedList;

    public PRNodeWritable() { 
		this.page = -2;
		this.PRval = 0;
		this.connectedList = new ArrayList<Integer>();
    }
	public PRNodeWritable(int page){ //delete if not needed
		setPage(page);
		this.PRval = 0;
		this.connectedList = new ArrayList<Integer>();
	}
	public PRNodeWritable(int page, double PRval ){ //delete if not needed
		setPage(page);
		setPRval(PRval);
		this.connectedList = new ArrayList<Integer>();
	}
    public PRNodeWritable(PRNodeWritable page){ //make deep copy
		this.page = page.getPage();
		this.PRval = page.getPRval();
		this.connectedList = new ArrayList<Integer>(page.getConnectedList());
	}
	public int getPage(){
		return page;
	}
	public void setPage(int page){
		this.page = page;
	}

	public double getPRval(){
		return PRval;
	}
	public void setPRval(double PRval){
		this.PRval = PRval;
	}

	public ArrayList<Integer> getConnectedList(){
		return connectedList;
	}
	public void setConnectedList(ArrayList<Integer> connectedList){
		this.connectedList = connectedList;
	}
    public void addConnectedList(int page){
		this.connectedList.add(page);
	}
	public String toString(){
		String output = Integer.toString(page) + " " + Double.toString(PRval)
			+ " " + Integer.toString(connectedList.size());
		for(int i = 0; i < connectedList.size(); ++i){
			output += " " + Integer.toString(connectedList.get(i));
		}
		return output;
	}
	//add additional functions if needed
	public void write(DataOutput out) throws IOException {
    	out.writeInt(page);
		out.writeDouble(PRval);
		out.writeInt(connectedList.size());
		for(int i = 0; i < connectedList.size(); ++i){
			out.writeInt(connectedList.get(i));
		}
    }

    public void readFields(DataInput in) throws IOException {
		page = in.readInt();
		PRval = in.readDouble();
		int size = in.readInt();
		this.connectedList = new ArrayList<Integer>();
		for (int i = 0 ; i < size ; i++){
			connectedList.add(in.readInt());
		}
    }
}