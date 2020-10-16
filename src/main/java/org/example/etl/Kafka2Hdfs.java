package org.example.etl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.example.util.Constant;

import java.util.Properties;

/**
 * 1.落盘数据到hdfs
 * 2.统计实时数据量：
 *  a.总刷卡次数
 *  b.刷卡人数---通行人数
 * @Author: morris
 * @Date: 2020/10/16 14:06
 * @reviewer
 */
public class Kafka2Hdfs {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        // 指定Kafka的连接位置
        properties.setProperty("bootstrap.servers", Constant.KAFKA_HOST);
        // 指定监听的主题，并定义Kafka字节消息到Flink对象之间的转换规则
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>(Constant.KAFKA_TOPIC, new SimpleStringSchema(), properties));
        //格式转换
        SingleOutputStreamOperator<String> flatMap = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                JSONArray data = JSONObject.parseObject(s).getJSONArray("data");
                for (Object o :
                        data) {
                    //过滤缺失数据
                    JSONObject jsonObject = (JSONObject) o;
                    if (jsonObject.size() != 11) {
                        continue;
                    }
                    String string = o.toString();
                    collector.collect(string);
                }
            }
        });
        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFKA_HOST);
        //key 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        flatMap.addSink(new FlinkKafkaProducer011<String>(
                Constant.KAFKA_HOST,
                Constant.KAFKA_TOPIC_REAL_TIME,
                String.class
                )).name("flink-connectors-kafka")
                .setParallelism(parameterTool.getInt("stream.sink.parallelism"))

//        stream.print();
        env.execute();

    }
}
