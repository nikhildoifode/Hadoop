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

public class Covid19_2 {

  public static class TokenizerMapper extends Mapper <Object, Text, Text, IntWritable> {
    private static IntWritable count;
    private Text word = new Text();
    private Date start = null, end = null;
    private SimpleDateFormat userInputDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();

      try {
        this.start = userInputDateFormat.parse(conf.get("start"));
        this.end = userInputDateFormat.parse(conf.get("end"));
      } catch (ParseException e) {
        e.printStackTrace();
      }

      super.setup(context);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String input = value.toString();
      String[] row = input.split(",");
      Date current = null;

      if (!(row[0].equals("date"))) {
        try {
          current = userInputDateFormat.parse(row[0]);
          if ((current.after(start) && current.before(end)) || current.equals(start) || current.equals(end)) {
            word.set(row[1]);
            count = new IntWritable(Integer.parseInt(row[3]));
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

    if (args.length != 4) {
      System.err.println("Insufficient or Extra arguments.");
      System.err.println("Usage: hadoop jar Covid19.jar Covid19_2 <in> [start_date yyyy-MM-dd] [end_date yyyy-MM-dd] <out>");
      System.exit(1);
    }

    SimpleDateFormat userInputDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    Date low = null, high = null, start = null, end = null;

    try {
      low = userInputDateFormat.parse("2019-12-31");
      high = userInputDateFormat.parse("2020-04-08");
      start = userInputDateFormat.parse(args[1]);
      end = userInputDateFormat.parse(args[2]);

      if (end.after(high) || end.before(low) || start.before(low) || start.after(high) || start.after(end)) {
        System.err.println("Data only available from 2019-12-31 to 2020-04-08. Input proper range");
        System.exit(1);
      }
    } catch (ParseException e) {
      System.err.println("Date formats are invalid! It should be yyyy-MM-dd");
      System.exit(1);
    }

    conf.set("start", args[1]);
    conf.set("end", args[2]);

    Job job = Job.getInstance(conf, "covid-19-2 analysis");
    job.setJarByClass(Covid19_2.class);
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

    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    if (hdfs.exists(new Path(args[3]))) hdfs.delete(new Path(args[3]), true);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
