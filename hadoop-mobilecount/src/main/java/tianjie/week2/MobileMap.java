package tianjie.week2;

import java.io.IOException;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MobileMap extends Mapper<LongWritable, Text, Text, MobileBean> {
    public MobileMap() {
    }

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MobileBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        MobileBean bean = new MobileBean();
        bean.setMobileNum(words[1].trim());
        bean.setDownLink(words[words.length - 3].trim());
        bean.setDownLink(words[words.length - 2].trim());
        Text outKey = new Text(bean.getMobileNum());
        context.write(outKey, bean);
    }
}