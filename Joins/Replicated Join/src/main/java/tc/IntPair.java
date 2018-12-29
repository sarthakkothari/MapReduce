package tc;

import java.io.*; import org.apache.hadoop.io.*;
public class IntPair implements WritableComparable<IntPair> {
    private int first; private int second; private char c;
    public IntPair() {}
    public IntPair(int first, int second) { set(first, second); }
    public IntPair(int first, int second, char c) { set(first, second, c); }
    public void set(int first, int second, char c) { this.first = first; this.second = second; this.c = c; }
    public void set(int first, int second) { this.first = first; this.second = second; }
    public int getFirst() { return first; }
    public int getSecond() { return second; }
    public char getType() { return c;}
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first); out.writeInt(second); out.writeChars(""+c); }
    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt(); second = in.readInt(); c=in.readChar(); }
    @Override
    public int hashCode() { return first * 163 + second; }
    @Override
    public boolean equals(Object o) {
        if (o instanceof IntPair) {
            IntPair ip = (IntPair) o;
            return first == ip.first && second == ip.second;
        }
        return false;
    }

    @Override
    public String toString() { return first + "\t" + second; }
    @Override
    public int compareTo(IntPair ip) {
        int cmp = compare(first, ip.first);
        if (cmp != 0) {
            return cmp;
        }
        return compare(second, ip.second);
    }
    /**
     * Convenience method for comparing two ints.
     */
    public static int compare(int a, int b) {
        return (a < b ? -1 : (a == b ? 0 : 1));
    }
}
