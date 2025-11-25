package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CategoryCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text category = new Text();
    private ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JsonNode node = jsonMapper.readTree(value.toString());
            String cat = node.get("category").asText();
            category.set(cat);
            context.write(category, one);
        } catch (Exception e) {
            // Ignore malformed JSON
        }
    }
}
