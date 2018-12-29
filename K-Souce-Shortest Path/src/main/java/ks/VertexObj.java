package ks;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.*;
import com.google.gson.Gson;

public class VertexObj implements WritableComparable<VertexObj> {
	private static final Gson gson = new Gson();
    private int id; ArrayList<Integer> adjList; private boolean active; private boolean isOb;
    private HashMap<Integer, Integer> map;
    public VertexObj() {}
    public VertexObj(VertexObj ob){
    		this.id = ob.getId() ; this.adjList = ob.getAdjList(); this.active = ob.getStatus(); this.map = ob.getMap(); this.isOb = ob.isObject(); }
    public VertexObj(int id, HashMap<Integer, Integer> map) { set(id, map); }
    public VertexObj(int id, ArrayList<Integer> adjList, boolean active) { set(id, adjList, active); }
    public VertexObj(int id, ArrayList<Integer> adjList, boolean active, boolean isOb, HashMap<Integer, Integer> map) { set(id, adjList, active, isOb, map); }
    public VertexObj(int id, ArrayList<Integer> adjList, boolean active, HashMap<Integer, Integer> map) { set(id, adjList, active, map); }
    
    public void set(int id, HashMap<Integer, Integer> map) { 
	  this.id = id; this.adjList = new ArrayList<Integer>(); this.map = map; this.isOb = false; this.active = false;}
    
    public void set(int id, ArrayList<Integer> adjList, boolean active) { 
    		this.id = id; this.adjList = adjList; this.active = active; this.map = new HashMap<Integer, Integer>(); isOb = false; }   
    
    public void set(int id, ArrayList<Integer> adjList, boolean active, boolean isOb, HashMap<Integer, Integer> map) { 
		this.id = id; this.adjList = adjList; this.map = map; this.isOb = false; this.active=active;}
    
    public void set(int id, ArrayList<Integer> adjList, boolean active, HashMap<Integer, Integer> map) { 
		this.id = id; this.adjList = adjList; this.map = map; isOb = false;}
    
    
    public int getId() { return id; }
    public ArrayList<Integer> getAdjList() { return adjList; }
    public boolean getStatus() { return active;}
    public void setStatus(boolean active) { this.active = active;}
    public boolean isObject() { return isOb; }
    public void setObject(boolean ob) { this.isOb = ob; }
    public HashMap<Integer, Integer> getMap() { return map; }
    @Override
    public void write(DataOutput out) throws IOException {
    		WritableUtils.writeString(out, gson.toJson(this)); 
    }
    @Override
    public void readFields(DataInput in) throws IOException {
    		VertexObj ob  = gson.fromJson(WritableUtils.readString(in), VertexObj.class);
    		this.id = ob.getId();
    		this.adjList = ob.getAdjList();
    		this.active = ob.getStatus();
    		this.map = ob.getMap();
    		this.isOb = ob.isObject();
    	}
    @Override
    public int hashCode() { return id * 163 ; }
    @Override
    public boolean equals(Object o) {
        if (o instanceof VertexObj) {
        		VertexObj ip = (VertexObj) o;
            return id == ip.id && adjList.equals(ip.adjList) && active == ip.active;
        }
        return false;
    }

    @Override
    public String toString() { return gson.toJson(this);  }
    @Override
    public int compareTo(VertexObj ip) {
         return compare(id, ip.id);
    }
    /**
     * Convenience method for comparing two ints.
     */
    public static int compare(int a, int b) {
        return (a < b ? -1 : (a == b ? 0 : 1));
    }
}
