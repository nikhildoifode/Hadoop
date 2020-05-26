import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class Covid19_1 {

  public static class TokenizerMapper extends Mapper <Object, Text, Text, IntWritable> {
    private static IntWritable count;
    private Text word = new Text();
    private boolean world;
    private Date dec31 = null;
    private SimpleDateFormat userInputDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();

      try {
        this.world = Boolean.parseBoolean(conf.get("world"));
        this.dec31 = userInputDateFormat.parse("2019-12-31");
      } catch (ParseException e) {
        e.printStackTrace();
      }

      super.setup(context);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String input = value.toString();
      String[] row = input.split(",");
      Date current = null;
      if (!(row[0].equals("date") || (!world && (row[1].equals("World") || row[1].equals("International"))))) {
        try {
          current = userInputDateFormat.parse(row[0]);
          if (!current.equals(dec31)) {
            word.set(row[1]);
            count = new IntWritable(Integer.parseInt(row[2]));
            context.write(word, count);
          }
        } catch (ParseException e) {
          e.printStackTrace();
        }
      }
    }
  }
  
  public static class IntSumReducer extends Reducer <Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    if (args.length != 3) {
      System.out.println("Insufficient or Extra arguments.");
      System.err.println("Usage: hadoop jar Covid19.jar Covid19_1 <in> [true|false] <out>");
      System.exit(1);
    }

    if (!(args[1].equals("true") || args[1].equals("false"))) {
      System.out.println("Invalid arguments. Either true or false");
      System.exit(1);
    }

    conf.set("world", args[1]);

    Job job = Job.getInstance(conf, "covid-19-1 analysis");
    job.setJarByClass(Covid19_1.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileSystem hdfs = FileSystem.get(conf);
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
