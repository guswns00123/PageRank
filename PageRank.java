import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapred.Counters;

public class PageRank {
    
    public static class PageRankMapper extends Mapper<LongWritable, Text, IntWritable, PRNodeWritable> {

        
        double init_PR;
        String first_itr;
        public void setup(Context context){
            Configuration conf = context.getConfiguration(); //use this to pass parameter         
            first_itr = conf.get("first_itr");
            init_PR = 1 / (Double.parseDouble(conf.get("total_page")));
        
        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            PRNodeWritable currentpage = new PRNodeWritable();
            //page cnt_connected connectedpages
            StringTokenizer itr = new StringTokenizer(value.toString()); //fromString
            itr.nextToken(); //node ID
            
            currentpage.setPage(Integer.parseInt(itr.nextToken()));
            double PR_val = Double.parseDouble(itr.nextToken());
            
            // N total node
            if (first_itr.equals("true")){
                PRNodeWritable tmp = new PRNodeWritable(-2, init_PR);
                context.write(new IntWritable(currentpage.getPage()), tmp);
                int size = Integer.parseInt(itr.nextToken());
                for(int i = 0; i < size; ++i){ //for all neighbor of current node
                    int connectedpage = Integer.parseInt(itr.nextToken());
                    currentpage.addConnectedList(connectedpage);
                }
            }else{
                currentpage.setPRval(PR_val);
                int size = Integer.parseInt(itr.nextToken());
                for (int i = 0; i < size ; i++){
                    int connectedpage = Integer.parseInt(itr.nextToken());
                    currentpage.addConnectedList(connectedpage);
                    double prval = PR_val / size;
                    PRNodeWritable tmp =  new PRNodeWritable(-2, prval);
                    context.write(new IntWritable(connectedpage), tmp);
                    
                }
            }
            
            context.write(new IntWritable(currentpage.getPage()), currentpage);
        }
    }

    public static class PageRankReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, Text> {        
        
        public void setup(Context context){
            Configuration conf = context.getConfiguration(); 
            
        }
        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context)
                throws IOException, InterruptedException {
            
            PRNodeWritable page = new PRNodeWritable();
            double s = 0;
            for (PRNodeWritable p : values) { //tmp
                if(key.get() == p.getPage()){ //if d is the passed on node struct
                    page = new PRNodeWritable(p); //deep copy
                }else{
                    s += p.getPRval();
                }
            }
            page.setPRval(s);
            
            context.write(key, new Text(page.toString()));            
        }
        
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf1 = new Configuration();
		conf1.set("iterations", args[0]);
        conf1.set("threshold", args[1]);
		Job job1 = Job.getInstance(conf1,"preprocess");
		job1.setJarByClass(PageRank.class);
		job1.setJar("pagerank.jar");
        job1.setMapperClass(PRPreProcess.PreprocessMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(PRPreProcess.PreprocessReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class); 
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        
		Path output = new Path(args[3]);
        String tmpPath = output.getParent().toString() + "/tmp";
		Path outputJob1 = new Path(tmpPath + "/tmp0");
		FileInputFormat.addInputPath(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, outputJob1);
		FileSystem fs = FileSystem.get(conf1);
		if (fs.exists(outputJob1)) {
			System.out.println("deleted folder: " + "/tmp0");
			fs.delete(outputJob1, true);
		}
        //System.exit(job1.waitForCompletion(true) ? 0 : 1);
        int i = 0;
		if(job1.waitForCompletion(true)) {
			System.out.println("start iteration!");
            Long Counter1 = job1.getCounters().findCounter(PRPreProcess.NodeCounter.COUNT).getValue();
            Configuration conf2 = new Configuration();
            conf2.set("total_page", Long.toString(Counter1));
            conf2.set("first_itr", "true");
            
			
			int iteration = Integer.parseInt(args[0]);
			for (i = 0 ; i < iteration; i = i + 1 ){
                Job job2 = Job.getInstance(conf2, "PageRank");
                job2.setJarByClass(PageRank.class);
				job2.setJar("pagerank.jar");
                
				job2.setMapperClass(PageRank.PageRankMapper.class);
                job2.setNumReduceTasks(1);
				// job.setCombinerClass(IntSumReducer.class);
				job2.setReducerClass(PageRank.PageRankReducer.class);

				job2.setMapOutputKeyClass(IntWritable.class);
				job2.setMapOutputValueClass(PRNodeWritable.class); 
				job2.setOutputKeyClass(IntWritable.class);
				job2.setOutputValueClass(Text.class);
				Path outputJob2 = new Path(tmpPath + "/tmp" + Integer.toString(i + 1));

				FileInputFormat.addInputPath(job2, new Path(tmpPath + "/tmp" + Integer.toString(i)));
				FileOutputFormat.setOutputPath(job2, outputJob2);
            
            
				
				if (fs.exists(outputJob2)) {
					System.out.println("deleted folder: /tmp" + Integer.toString(i + 1));
					fs.delete(outputJob2, true);
				}
				if(job2.waitForCompletion(true)) {;
					System.out.println("iteration: " + Integer.toString(i + 1));
					
				}else {
					System.out.println("failed");
				}	
                conf2.set("first_itr", "false");
                
            }
			
		}
        Job job3 = Job.getInstance(conf1, "Page Rank");
        job3.setJarByClass(PageRank.class);
		job3.setJar("pagerank.jar");
        job3.setMapperClass(PRPreProcess.ResultMapper.class);
        
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);
		Path outputJob3 = new Path(args[3]);
        FileInputFormat.addInputPath(job3, new Path(tmpPath + "/tmp" + Integer.toString(i)));
        FileOutputFormat.setOutputPath(job3, outputJob3);

		if (fs.exists(outputJob3)) {
					System.out.println("deleted folder: /final");
					fs.delete(outputJob3, true);
				}
		if(job3.waitForCompletion(true)){
			System.out.println("job3 completed, final result in output/");
			System.exit(0);
		}else{
			System.exit(1);
		}
	}
}