import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

public class SelectJoin {
    public static final String DELIMITER = ",";

    static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineValue = value.toString();
            String[] values = lineValue.split(DELIMITER);

            int length = values.length;
            if(length != 4 && length != 3){
                return;
            }

            if(length == 4){
                String p_id = values[0];
                String p_name = values[1];
                String p_sex = values[2];
                String c_id = values[3];

                context.write(new Text(c_id), new Text("person" + DELIMITER + p_id + DELIMITER + p_name + DELIMITER + p_sex));
            }

            if(length == 3){
                String c_id = values[0];
                String c_name = values[1];
                String c_time = values[2];

                context.write(new Text(c_id), new Text("course" + DELIMITER + c_name + DELIMITER + c_time));
            }
        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            LinkedList<String> personList = new LinkedList<>();
            LinkedList<String> courseList = new LinkedList<>();

            for(Text tvalue : values){
                String value = tvalue.toString();
                if (value.startsWith("person")){
                    personList.add(value.substring(7));
                }else if(value.startsWith("course")){
                    courseList.add(value.substring(6));
                }
            }

            for (String person : personList){
                for (String course : courseList){
                    context.write(new Text(person.split(DELIMITER)[0]),new Text(person + course));
                }
            }
        }

    }

    private final static String FILE_IN_PATH = "src/data1";
    private final static String FILE_OUT_PATH = "src/dataoutput1";

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, FILE_IN_PATH);

        File f = new File(FILE_OUT_PATH);
        if (f.exists()){
            FileUtils.deleteDirectory(f);
        }
        FileOutputFormat.setOutputPath(job, new Path(FILE_OUT_PATH));

        job.waitForCompletion(true);
    }


}
