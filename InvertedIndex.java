import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
  private static final Pattern WORD_PATTERN = Pattern.compile("[A-Za-z]+");

  public static class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    @Override
    protected boolean isSplitable(org.apache.hadoop.mapreduce.JobContext context, Path filename) {
      return false;
    }

    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(
        InputSplit split,
        TaskAttemptContext context) {
      return new WholeFileRecordReader();
    }
  }

  public static class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit fileSplit;
    private Configuration conf;
    private boolean processed = false;
    private final NullWritable key = NullWritable.get();
    private final BytesWritable value = new BytesWritable();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
      this.fileSplit = (FileSplit) split;
      this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      if (processed) {
        return false;
      }

      byte[] contents = new byte[(int) fileSplit.getLength()];
      Path file = fileSplit.getPath();
      FileSystem fs = file.getFileSystem(conf);

      try (FSDataInputStream in = fs.open(file)) {
        in.readFully(0, contents);
      }

      value.set(contents, 0, contents.length);
      processed = true;
      return true;
    }

    @Override
    public NullWritable getCurrentKey() {
      return key;
    }

    @Override
    public BytesWritable getCurrentValue() {
      return value;
    }

    @Override
    public float getProgress() {
      return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() {
    }
  }

  public static class IndexMapper extends Mapper<NullWritable, BytesWritable, Text, Text> {
    private final Set<String> stopwords = new HashSet<>();
    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
      URI[] cacheFiles = context.getCacheFiles();
      if (cacheFiles == null || cacheFiles.length == 0) {
        throw new IOException("Missing stopwords file. Pass it as the third argument.");
      }

      Path stopwordsPath = new Path(cacheFiles[0].getPath());
      FileSystem fs = FileSystem.get(context.getConfiguration());

      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(fs.open(stopwordsPath), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String word = line.trim().toLowerCase();
          if (!word.isEmpty()) {
            stopwords.add(word);
          }
        }
      }
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context)
        throws IOException, InterruptedException {
      FileSplit split = (FileSplit) context.getInputSplit();
      String filename = split.getPath().getName();
      String fileContent = new String(value.getBytes(), 0, value.getLength(), StandardCharsets.UTF_8);
      String[] lines = fileContent.split("\\R", -1);

      for (int i = 0; i < lines.length; i++) {
        int lineNumber = i + 1;
        Matcher matcher = WORD_PATTERN.matcher(lines[i]);

        while (matcher.find()) {
          String word = matcher.group().toLowerCase();
          if (stopwords.contains(word)) {
            continue;
          }

          outputKey.set(word);
          outputValue.set(filename + ":" + lineNumber);
          context.write(outputKey, outputValue);
        }
      }
    }
  }

  public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      Map<String, Set<Integer>> locations = new TreeMap<>();

      for (Text value : values) {
        String[] parts = value.toString().split(":", 2);
        if (parts.length != 2) {
          continue;
        }

        String filename = parts[0];
        int lineNumber = Integer.parseInt(parts[1]);
        locations.computeIfAbsent(filename, ignored -> new TreeSet<>()).add(lineNumber);
      }

      StringBuilder builder = new StringBuilder();
      for (Map.Entry<String, Set<Integer>> entry : locations.entrySet()) {
        if (builder.length() > 0) {
          builder.append(" ");
        }

        builder.append("(").append(entry.getKey());
        for (Integer lineNumber : entry.getValue()) {
          builder.append(", ").append(lineNumber);
        }
        builder.append(")");
      }

      result.set(builder.toString());
      context.write(key, result);
    }
  }

  private static void addInputFiles(FileSystem fs, Job job, Path inputPath) throws IOException {
    if (fs.getFileStatus(inputPath).isFile()) {
      FileInputFormat.addInputPath(job, inputPath);
      return;
    }

    RemoteIterator<LocatedFileStatus> files = fs.listFiles(inputPath, false);
    while (files.hasNext()) {
      LocatedFileStatus file = files.next();
      if (file.isFile()) {
        FileInputFormat.addInputPath(job, file.getPath());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: InvertedIndex <input> <output> <stopwords>");
      System.exit(2);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted index");
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    Path stopwordsPath = new Path(args[2]);
    FileSystem fs = inputPath.getFileSystem(conf);

    job.setJarByClass(InvertedIndex.class);
    job.setInputFormatClass(WholeFileInputFormat.class);
    job.setMapperClass(IndexMapper.class);
    job.setReducerClass(IndexReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.addCacheFile(stopwordsPath.toUri());

    addInputFiles(fs, job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
