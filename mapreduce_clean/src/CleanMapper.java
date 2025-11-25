package mapreduce_clean;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private ObjectMapper json = new ObjectMapper();
    private Text cleaned = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            JsonNode node = json.readTree(value.toString());

            String category = node.get("category").asText();
            String date = node.get("date").asText();
            String authors = node.get("authors").asText();
            String headline = node.get("headline").asText();
            String shortDesc = node.get("short_description").asText();

            if (category.isEmpty() || date.isEmpty() || headline.isEmpty()) return;

            String out = category + "\t" + date + "\t" +
                         authors + "\t" + headline + "\t" + shortDesc;

            cleaned.set(out);
            context.write(NullWritable.get(), cleaned);

        } catch (Exception e) {
            // skip invalid JSON
        }
    }
}
