package cse6322.hw1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgByKeyWithCombiner {

  private static SumCountWritable aggregate(Iterable<SumCountWritable> values) {
    double sum = 0.0;
    long count = 0;
    for (SumCountWritable sc : values) {
      sum += sc.getSum();
      count += sc.getCount();
    }
    return new SumCountWritable(sum, count);
  }

  public static class AvgMapper extends Mapper<LongWritable, Text, Text, SumCountWritable> {
    private final Text outKey = new Text();
    private final SumCountWritable outVal = new SumCountWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString().trim();
      if (line.isEmpty()) return;

      String[] parts = line.split(",");
      if (parts.length != 2) return;

      String k = parts[0].trim();
      double v;
      try {
        v = Double.parseDouble(parts[1].trim());
      } catch (NumberFormatException e) {
        return;
      }

      outKey.set(k);
      outVal.set(v, 1);
      context.write(outKey, outVal);
    }
  }

  public static class SumCountCombiner extends Reducer<Text, SumCountWritable, Text, SumCountWritable> {
    private final SumCountWritable out = new SumCountWritable();

    @Override
    protected void reduce(Text key, Iterable<SumCountWritable> values, Context context)
        throws IOException, InterruptedException {
      SumCountWritable agg = aggregate(values);
      out.set(agg.getSum(), agg.getCount());
      context.write(key, out);
    }
  }

  public static class AvgReducer extends Reducer<Text, SumCountWritable, Text, DoubleWritable> {
    private final DoubleWritable out = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<SumCountWritable> values, Context context)
        throws IOException, InterruptedException {
      SumCountWritable agg = aggregate(values);
      if (agg.getCount() == 0) return;
      out.set(agg.getSum() / agg.getCount());
      context.write(key, out);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: AvgByKeyWithCombiner <input> <output>");
      System.exit(1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "AvgByKeyWithCombiner");

    job.setJarByClass(AvgByKeyWithCombiner.class);

    job.setMapperClass(AvgMapper.class);
    job.setCombinerClass(SumCountCombiner.class);
    job.setReducerClass(AvgReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(SumCountWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
