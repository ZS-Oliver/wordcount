package hadoop.mapreduce.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    // 框架在map处理完成后，将所有kv对缓存起来，进行分组，然后传递一个组<key,values>,再调用reduce方法
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long count = 0;
        for (LongWritable value : values) {

            count += value.get();
        }

        context.write(key, new LongWritable(count));
    }
}
