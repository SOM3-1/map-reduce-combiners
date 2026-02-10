package cse6322.hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

public class AvgByKeyWithHashtable {

  public static class InMapperAvg extends Mapper<LongWritable, Text, Text, SumCountWritable> {
    private final Map<String, SumCountWritable> table = new HashMap<>();
    private final Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) {
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

      SumCountWritable cur = table.get(k);
      if (cur == null) {
        table.put(k, new SumCountWritable(v, 1));
      } else {
        cur.set(cur.getSum() + v, cur.getCount() + 1);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      SumCountWritable outVal = new SumCountWritable();
      for (Map.Entry<String, SumCountWritable> e : table.entrySet()) {
        outKey.set(e.getKey());
        SumCountWritable sc = e.getValue();
        outVal.set(sc.getSum(), sc.getCount());
        context.write(outKey, outVal);
      }
    }
  }

  public static class AvgReducer extends Reducer<Text, SumCountWritable, Text, DoubleWritable> {
    private final DoubleWritable out = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<SumCountWritable> values, Context context)
        throws IOException, InterruptedException {
      double sum = 0.0;
      long count = 0;
      for (SumCountWritable sc : values) {
        sum += sc.getSum();
        count += sc.getCount();
      }
      if (count == 0) return;
      out.set(sum / count);
      context.write(key, out);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: AvgByKeyWithHashtable <input> <output>");
      System.exit(1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "AvgByKeyWithHashtable");

    job.setJarByClass(AvgByKeyWithHashtable.class);

    job.setMapperClass(InMapperAvg.class);
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
