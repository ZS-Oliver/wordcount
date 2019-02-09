package hadoop.mapreduce.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// 前两个指定mapper输入数据的类型，KEYIN为输入的key类型，VALUEYIN是输入的value的类型
// 后两个指定mapper输出的类型，KEYOUT为输出的key类型，VALUEOUT为输出的value类型
// map和reduce的数据输入输出都是以key-value对的形式封装的

// LongWriteable为hadoop为减小序列化后大小所对Long做的封装，同理，Text对应String
public class WCMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    // mapreduce框架每读一次数据就调用一次该方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // key是这一行数据的起始偏移量，value是这一行的文本内容
        String line = value.toString();

        String[] words = StringUtils.split(line, " ");

        for(String word : words){
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
