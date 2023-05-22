import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.Counters;

public class PRPreProcess {
    public static enum NodeCounter{COUNT};
    public static class PreprocessMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString());
            String first = itr.nextToken();
            String sec = itr.nextToken();
            
            context.write(new Text(first), new Text(sec));
        }
    }

    
    public static class ResultMapper extends Mapper<Object, Text, IntWritable, Text> {
        double threshold;
        public void setup(Context context){
            Configuration conf = context.getConfiguration(); //use this to pass parameter         
            threshold = Double.parseDouble(conf.get("threshold"));
            
        
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString());
            itr.nextToken();
            int page = Integer.parseInt(itr.nextToken());
            String prval = itr.nextToken();
            double prval1 = Double.parseDouble(prval);
            if (prval1 > threshold){
                context.write(new IntWritable(page), new Text(prval));
            }
        }
    }
    public static class PreprocessReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        HashMap<Integer, Integer> pageMap = new HashMap<Integer, Integer>();
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            
            pageMap.put(key.get(),1);
            String output1 = Integer.toString(key.get()) + " 0 ";
            int cntConnected = 0; // the number of connect of node
            String output2 = "";
            for(IntWritable val : values){
                cntConnected++;
                int linkedpage = val.get();
                output2 = output2 + " " + Integer.toString(linkedpage);
                if (!pageMap.containsKey(linkedpage)){
                    pageMap.put(linkedpage, 0);
                }
            }
            String outputFinal = output1 + Integer.toString(cntConnected) + output2;
            context.getCounter(NodeCounter.COUNT).increment(1);
            context.write(key, new Text(outputFinal)); //page cnt_connected linkedpage
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, Integer> page : pageMap.entrySet()){
                int page_node = page.getKey();
                int flag = page.getValue();
                if (flag == 0){
                    context.getCounter(NodeCounter.COUNT).increment(1);
                    String output = Integer.toString(page_node) + " 0 0";
                    context.write(new IntWritable(page_node), new Text(output));
                }
                
            }
        }
    }
}
