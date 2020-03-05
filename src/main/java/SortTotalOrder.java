import java.io.IOException;
import java.net.URI;
import java.security.Key;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortOrder {

    public static void main(String[] args) throws Exception{

        // Set the number of reducer (No more than 10)
        int reduceNumber = 10;

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: Patent <in> <out>");
            System.exit(2);
        }


        conf.setInt("Count", 0);

        // Use InputSampler.RandomSampler to create sample list
        // Although the efficiency of RandomSampler is the lowest in all three kinds of Sampler (interval, splitSampler)
        // But randomSampler is the most accurate sampler class for this experiment
        // The argument set as the following:
        // freq: 0.1 (probability with which a key will be chosen)
        // numSamplers: 5000000 (total number of samples to obtain from all splits)
        // maxSplitSampled: 9 (the maximum number of splits to examine)
        RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text> (0.15, 5600000);

        // Set the path of partition file
        TotalOrderPartitioner.setPartitionFile(conf, new Path("home/lab3/_partitions"));


        Job job = Job.getInstance(conf, "Exp1");
        job.setJarByClass(SortTotalOrder.class);
        job.setNumReduceTasks(reduceNumber);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(mapOne.class);
        job.setReducerClass(reduceOne.class);

        // KeyValueTextInputFormat.class is the only one class we can use in this experiment
        // Since TotalOrderPartitioner.class requires input must be <Text, Text>
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //SequenceFileOutputFormat.setCompressOutput(job, true);
        //SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        //SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

        // Use TotalOrderPartitioner class to sort input data

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

        // Output path
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));


        if (job.waitForCompletion(true)) {

			/*
			FileSystem fs = FileSystem.get(conf);
			// Create path object and check for its existence
			Path ParPath = new Path("/user/xuteng/lab4/_partitions");
			if (fs.exists(ParPath)) {
				// false indicates do not deletes recursively
				fs.delete(ParPath, false);

			  }
			  */

            System.exit(0);
        }
        //job.waitForCompletion(true);

        ////////////////////////////////////////////////////////////////////////////
    }

    public static class mapOne extends Mapper<Text, Text, IntWritable, Text> {
        private IntWritable num = new IntWritable();
        private Text pairText = new Text();

//begin sampling
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
           String pair = key.toString() + " " + value.toString();
           num.set((int)Math.round(Math.random() * 1000000));
           pairText.set(pair);
           context.write(num, pairText);
        }
    }

    public static class reduceOne extends Reducer<IntWritable, Text, Text, Text> {
        private Text keyText = new Text();
        private Text valText = new Text();
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                String[] keyPair = val.toString().split(" ");
                keyText.set(keyPair[0]);
                valText.set(keyPair[1]);
                context.write(keyText, valText);
            }
        }
    }
}
