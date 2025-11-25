import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NewsPreprocessMain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Pass the stop words file path to the Mapper
        // args[2] is expected to be the HDFS path to stopwords.txt
        if (args.length > 2) {
            conf.set("stopWordsPath", args[2]);
        }

        Job job = Job.getInstance(conf, "News Preprocessing");
        
        job.setJarByClass(NewsPreprocessMain.class);
        job.setMapperClass(NewsPreprocessMapper.class);
        job.setReducerClass(NewsPreprocessReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}