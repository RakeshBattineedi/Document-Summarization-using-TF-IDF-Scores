import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


public class profilea {
    public enum TOTAL_DOCUMENTS {
        N
    };


    public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!(value.toString().isEmpty())) {
                String[] t1 = value.toString().split("<====>");
                if (!(t1[1].isEmpty())) {
                    StringTokenizer itrID = new StringTokenizer(t1[1]);
                    String DocID = itrID.nextToken().toString();
                    if(t1.length == 3) {
                        StringTokenizer itr = new StringTokenizer(t1[2]);
                        while (itr.hasMoreTokens()) {
                            String out = itr.nextToken().replaceAll("[^A-Za-z0-9]","").toLowerCase();
                            if(!out.isEmpty()) {
                                String CompKey = DocID+"\t"+out;
                                context.write(new Text(CompKey.toString()), one);
                            }
                        }
                    }
                }
            }
        }
    }
    
    public static class partitioner1 extends Partitioner<Text, IntWritable>{
    	
    	public int getPartition(Text key, IntWritable value, int numPartitions) {
    		int hash= Math.abs(key.toString().hashCode());
    		return hash % numPartitions;
    	}
    }

    public static class Reducer1 extends Reducer<Text,IntWritable, Text, Text> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            String[] t1 = key.toString().split("\t");
            String DocIDFromInputKey = t1[0];
            String UnigramFromInputKey = t1[1];

            for (IntWritable val : values) {
                sum += val.get();
            }

            String fre = Integer.toString(sum);
            String CompValue = UnigramFromInputKey+"\t"+ fre;
            context.write(new Text(DocIDFromInputKey), new Text(CompValue));
        }

    }

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] t1 = value.toString().split("\t");
            String freFromInputKey = t1[2];
            String UnigramFromInputKey = t1[1];
            String CompValue = UnigramFromInputKey+"\t"+ freFromInputKey;
            context.write(new Text(t1[0]), new Text(CompValue));
        }
    }
public static class partitioner2 extends Partitioner<Text, Text>{
    	
    	public int getPartition(Text key, Text value, int numPartitions) {
    		int hash= Math.abs(key.toString().hashCode());
    		return hash % numPartitions;
    	}
    }
    
    public static class Reducer2 extends Reducer<Text,Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> list = new ArrayList<String>();
            int max = 0;
            int value = 0;
            int F=0;
            double TF=0;
            for (Text val : values) {

                list.add(val.toString());
                String[] t1 = val.toString().split("\t");
                value = Integer.parseInt(t1[1]);
                if (value > max) {
                    max = value;
                }
            }

            for (String newvalue  : list) {
                String[] t2 = newvalue.toString().split("\t");
                F = Integer.parseInt(t2[1]);
                TF = (0.5 + (0.5 * ((double)F/max)));
                String CompValue = t2[0]+"\t"+ Double.toString(TF);
                context.write( key, new Text(CompValue));

            }

            context.getCounter(TOTAL_DOCUMENTS.N).increment(1);

        }
    }

    public static class Mapper3 extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] t1 = value.toString().split("\t");
            String UnigramFromInputKey = t1[1];
            context.write(new Text(UnigramFromInputKey), new Text(value));
        }
    }
    
public static class partitioner3 extends Partitioner<Text, Text>{
    	
    	public int getPartition(Text key, Text value, int numPartitions) {
    		int hash= Math.abs(key.toString().hashCode());
    		return hash % numPartitions;
    	}
    }
    

    public static class Reducer3 extends Reducer<Text,Text, Text, Text> {

        private long N;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            this.N  = context.getConfiguration().getLong(TOTAL_DOCUMENTS.N.name(), 0);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> list = new ArrayList<String>();
            int count = 0;
            double TF=0;
            double IDF=0;
            double TFIDF=0;
            for (Text val : values) {
                list.add(val.toString());
                count++;
            }

            for (String newvalue  : list) {
                String[] t2 = newvalue.toString().split("\t");
                TF = Double.parseDouble(t2[2]);
                IDF= Math.log10((double) N/count);
                TFIDF = TF * IDF ;
                String CompValue = t2[1]+"\t"+ Double.toString(TFIDF);
                context.write( new Text(t2[0]), new Text(CompValue));

            }


        }
    }




    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Job1");
        job1.setJarByClass(profilea.class);
        job1.setMapperClass(Mapper1.class);
        job1.setNumReduceTasks(10);
        job1.setPartitionerClass(partitioner1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "job2");
        job2.setJarByClass(profilea.class);
        job2.setMapperClass(Mapper2.class);
        job2.setNumReduceTasks(10);
        job2.setPartitionerClass(partitioner2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);
        Counter cn=job2.getCounters().findCounter(TOTAL_DOCUMENTS.N);


        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "job3");
        job3.getConfiguration().setLong(TOTAL_DOCUMENTS.N.name(), cn.getValue());
        job3.setJarByClass(profilea.class);
        job3.setMapperClass(Mapper3.class);
        job3.setNumReduceTasks(10);
        job3.setPartitionerClass(partitioner3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
        
        
        
        
        
        