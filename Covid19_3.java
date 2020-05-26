import java.io.IOException;
import java.util.HashMap;
import java.net.URI;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

public class Covid19_3 {

  public static class TokenizerMapper extends Mapper <Object, Text, Text, DoubleWritable> {
    private static DoubleWritable count;
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String input = value.toString();
      String[] row = input.split(",");

      if (!(row[0].equals("date"))) {
        word.set(row[1]);
        count = new DoubleWritable(Double.parseDouble(row[2]));
        context.write(word, count);
      }
    }
  }

  public static class IntSumReducer extends Reducer <Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    private HashMap <String, Integer> map = new HashMap<>();

    protected void setup(Reducer<Text,DoubleWritable,Text,DoubleWritable>.Context context) throws IOException, InterruptedException {
      URI[] files = context.getCacheFiles();
      for (URI file : files) {
        try {
          FSDataInputStream fsdis = FileSystem.get(new Configuration()).open(new Path(file));
          BufferedReader csvReader = new BufferedReader(new InputStreamReader(fsdis));
          String line = "";

          while ((line = csvReader.readLine()) != null) {
            String[] row = line.split(",");
            if (!(row.length != 5 || row[1].equals("location")))
              map.put(row[1], Integer.valueOf(row[4]));
          }

          csvReader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      Integer population = map.get(key.toString());
      if (population != null) {
        double sum = 0;
        for (DoubleWritable val : values) {
          sum += val.get();
        }

        result.set((sum / population.intValue()) * 1000000);
        context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    if (args.length != 3) {
      System.out.println("Insufficient or Extra arguments.");
      System.err.println("Usage: hadoop jar Covid19.jar Covid19_3 <in> <population_file> <out>");
      System.exit(1);
    }

    Job job = Job.getInstance(conf, "covid-19-3 analysis");
    job.setJarByClass(Covid19_3.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileSystem hdfs = FileSystem.get(conf);
    if (!hdfs.exists(new Path(args[1]))) {
      System.out.println(args[1] + " doesn't exist.");
      System.exit(1);
    }

    // job.addCacheFile(new Path("cse532/cache/populations.csv").toUri());
    job.addCacheFile(new Path(args[1]).toUri());

    String inputPath = args[0];
    if (hdfs.exists(new Path(inputPath))) FileInputFormat.addInputPath(job, new Path(inputPath));
    else {
      System.out.println(inputPath + " doesn't exist.");
      System.exit(1);
    }

    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    if (hdfs.exists(new Path(args[2]))) hdfs.delete(new Path(args[2]), true);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
