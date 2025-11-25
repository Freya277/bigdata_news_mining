import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.InputStreamReader;
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

        // Remove all non-Chinese characters
        String cleanedContent = content.replaceAll("[^\\u4e00-\\u9fa5]", "").trim();
        if (cleanedContent.isEmpty()) return;

        // Chinese word segmentation with IKAnalyzer
        StringReader reader = new StringReader(cleanedContent);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true); // true for smart segmentation
        Lexeme lexeme;
        StringBuilder segmentedContent = new StringBuilder();
        
        while ((lexeme = ikSegmenter.next()) != null) {
            String word = lexeme.getLexemeText();
            // Filter stopwords and single-character words
            if (!stopWords.contains(word) && word.length() > 1) {
                segmentedContent.append(word).append(" ");
            }
        }
        
        String finalContent = segmentedContent.toString().trim();
        if (finalContent.isEmpty()) return;

        // Calculate word count
        int wordCount = finalContent.split("\\s+").length;

        outKey.set(category);
        outValue.set(finalContent + "\t" + wordCount);

        context.write(outKey, outValue);
    }
}