package mapreduce_clean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CleanDriver {
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: CleanDriver <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "News_Clean");

        job.setJarByClass(CleanDriver.class);
        job.setMapperClass(CleanMapper.class);
        job.setReducerClass(CleanReducer.class);

        job.setOutputKeyClass(org.apache.hadoop.io.NullWritable.class);
        job.setOutputValueClass(org.apache.hadoop.io.Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
