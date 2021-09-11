import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top100Words {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    Map<String, Integer> map = new HashMap<>();
    String str = "";

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        str = itr.nextToken().replaceAll("[^A-Za-z]+","").toLowerCase();
        if (map.get(str) == null) {
          map.put(str, 1);
        } else {
          map.put(str, map.get(str) + 1);
        }
      }
    }

    private IntWritable count = new IntWritable();
    public void cleanup(Context context) throws IOException, InterruptedException {
      for (String s : map.keySet()) {
        word.set(s);
        count.set(map.get(s));
        context.write(word, count);
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,Text> {
    private IntWritable result = new IntWritable();
    
    public class Word {
      public String word;
      public int file;
      public int count;

      public Word(String w, int f, int c) {
        word = w;
        file = f;
        count = c;
      }
    }

    public class WordComparator implements Comparator<Word> {
      public int compare(Word w1, Word w2) {
        int file = w2.file - w1.file;
        if (file == 0) {
          return w2.count - w1.count;
        } else {
          return file;
        }
      }
    }
    
    ArrayList<Word> list = new ArrayList<>();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int file = 0;
      int sum = 0;

      for (IntWritable val : values) {
        sum += val.get();
        file++;
      }

      list.add(new Word(key.toString(), file, sum));
    }
    
    Text word = new Text();
    Text count = new Text();

    public void cleanup(Context context) throws IOException, InterruptedException {
      Collections.sort(list, new WordComparator());

      int limit = list.size() > 100 ? 100 : list.size();

      for (int i = 0; i < limit; i++) {
        word.set(list.get(i).word);
        count.set(list.get(i).file + "\t" + list.get(i).count);
        context.write(word, count);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Top100Words.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}