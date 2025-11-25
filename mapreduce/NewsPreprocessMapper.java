import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class NewsPreprocessMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();
    private Set<String> stopWords = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String stopWordsPath = context.getConfiguration().get("stopWordsPath");
        if (stopWordsPath != null) {
            Path path = new Path(stopWordsPath);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line != null && !line.trim().isEmpty()) {
                        stopWords.add(line.trim());
                    }
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line == null || line.trim().isEmpty()) return;

        String[] parts = line.split("\t", 2);
        if (parts.length != 2) return;

        String category = parts[0].trim();
        String content = parts[1].trim();

        String cleanedContent = content.replaceAll("[^\\u4e00-\\u9fa5\\s]", "").trim();
        if (cleanedContent.isEmpty()) return;

        String[] words = cleanedContent.split("\\s+");
        StringBuilder sb = new StringBuilder();
        for (String word : words) {
            if (!stopWords.contains(word)) {
                sb.append(word).append(" ");
            }
        }
        cleanedContent = sb.toString().trim();
        if (cleanedContent.isEmpty()) return;

        int wordCount = cleanedContent.split("\\s+").length;

        outKey.set(category);
        outValue.set(cleanedContent + "\t" + wordCount);

        context.write(outKey, outValue);
    }
}