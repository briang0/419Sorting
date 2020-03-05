import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
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

        // Set the path of partition file
//        TotalOrderPartitioner.setPartitionFile(conf, new Path("home/lab3/_partitions"));



        Job job = Job.getInstance(conf, "Exp1");;
        job.setJarByClass(SortOrder.class);
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
        FileOutputFormat.setOutputPath(job, new Path("/home/lab3/_partitions"));


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

        Job job2 = Job.getInstance(conf, "Special Name");
        job2.setJarByClass(SortOrder.class);
        job2.setNumReduceTasks(reduceNumber);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(mapOne.class);
        job2.setReducerClass(reduceOne.class);

        // KeyValueTextInputFormat.class is the only one class we can use in this experiment
        // Since TotalOrderPartitioner.class requires input must be <Text, Text>
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setPartitionerClass(MyPartitioner.class);

        FileInputFormat.addInputPath(job2, new Path("home/lab3/_partitions"));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
        //job.waitForCompletion(true);

        ////////////////////////////////////////////////////////////////////////////
    }

    public static class MyPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int i) {
            int first = key.toString().toLowerCase().charAt(0);
            if (i == 0) {
                return 0;
            } else if (i == 10) {
                if (first >= 48 && first <= 51) {
                    return 0;
                } else if (first >= 51 && first <= 54) {
                    return 1;
                } else if (first >= 55 && first <= 57) {
                    return 2;
                } else if (first >= 97 && first <= 99) {
                    return 3;
                } else if (first >= 100 && first <= 103) {
                    return 4;
                } else if (first >= 104 && first <= 107) {
                    return 5;
                } else if (first >= 108 && first <= 112) {
                    return 6;
                } else if (first >= 113 && first <= 116) {
                    return 7;
                } else if (first >= 117 && first <= 120) {
                    return 8;
                } else if (first >= 121 && first <= 122) {
                    return 9;
                }
            }
            return -1;
        }
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
