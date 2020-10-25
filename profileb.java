import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Logger;


public class profileb {

    //private static Logger LOGGER = Logger.getLogger(profileb.class.getName());

    public static class Mappertfidf extends Mapper<LongWritable, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] t1 = value.toString().split("\t");
            String DocIDFromInputKey = t1[0];
            String CompValue = t1[1] + "\t" + t1[2];
            outkey.set(DocIDFromInputKey);
            outvalue.set("A" + CompValue);
            context.write(outkey, outvalue);
        }
    }

    public static class Mappercontent extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!(value.toString().isEmpty())) {
                String[] t1 = value.toString().split("<====>");
                if (!(t1[1].isEmpty())) {
                    outkey.set(t1[1]);
                    if (t1.length == 3) {
                        outvalue.set("B" + t1[2]);
                        context.write(outkey, outvalue);
                    }
                }
            }
        }
    }
    
public static class partitioner extends Partitioner<Text, Text>{
    	
    	public int getPartition(Text key, Text value, int numPartitions) {
    		int hash= Math.abs(key.toString().hashCode());
    		return hash % numPartitions;
    	}
    }

    public static class Reducersummary extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text outvalue = new Text();
            HashMap<String, String> map = new HashMap<>();

            TreeMap<Integer, String> tmap = new TreeMap<>();

            TreeMap<Integer, Double> smap = new TreeMap<>();
            TreeMap<Integer, Double> smap1 = new TreeMap<>();
            String sent = "";
            for (Text val : values) {
                if (val.charAt(0) == 'A') {
                    String temp = val.toString().substring(1);
                    String[] t1 = temp.split("\t");
                    map.put(t1[0], t1[1]);
                }
                if (val.charAt(0) == 'B') {
                    String tmp = val.toString().substring(1);
                    String[] t2 = tmp.split("\\.+\\s");
                    for (int i = 0; i < t2.length; i++) {
                        tmap.put(i, t2[i]);
                    }
                }
            }
            if(!map.isEmpty()) {
                
                for (Map.Entry<Integer, String> entry : tmap.entrySet()) {
                    TreeMap<String, Double> unimap = new TreeMap<>();
                    String[] t3 = entry.getValue().split("\\s");
                    for (int i = 0; i < t3.length; i++) {
                        String out = t3[i].replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                        if (!out.isEmpty() && map.containsKey(out)) {
                            unimap.put(out, Double.parseDouble(map.get(out)));
                        }
                    }
                    List<Entry<String, Double>> unilist = new ArrayList<>(unimap.entrySet());
                    unilist.sort(Entry.<String, Double>comparingByValue().reversed());

                   
                    HashMap<String, Double> tempmap = new HashMap<>();
                    for (int i = 0; i < 5 && i < unilist.size(); i++) {
                        tempmap.put(unilist.get(i).getKey(),unilist.get(i).getValue());
                    }
                    
                    double sum = 0;
                    for (Map.Entry<String, Double> sentence : tempmap.entrySet()) {
                        sum = sum + sentence.getValue();
                    }
                    smap.put(entry.getKey(), sum);

                }
            }

            

            if (!smap.isEmpty()) {
                List<Entry<Integer, Double>> sumlist = new ArrayList<>(smap.entrySet());
                sumlist.sort(Entry.<Integer, Double>comparingByValue().reversed());
                for (int i = 0; i < 3 && i< sumlist.size(); i++) {
                    smap1.put(sumlist.get(i).getKey(), sumlist.get(i).getValue());
                }
              

                List<Entry<Integer, Double>> list = new ArrayList<>(smap1.entrySet());

                

                for (Map.Entry<Integer, Double> entry : list) {
                    sent = sent + tmap.get(entry.getKey()) + ". ";
                }

                outvalue.set("<====>"+ sent);
                context.write(key, outvalue);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "profileb");
        job.setJarByClass(profileb.class);
        job.setNumReduceTasks(10);
        job.setPartitionerClass(partitioner.class);
        job.setReducerClass(Reducersummary.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Mappertfidf.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mappercontent.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}