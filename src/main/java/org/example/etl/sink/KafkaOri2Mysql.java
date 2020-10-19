package org.example.etl.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.util.Constant;
import org.example.util.KafkaUtil;

import java.util.Properties;

/**
 * @Author: morris
 * @Date: 2020/10/19 15:29
 * @reviewer
 */
public class KafkaOri2Mysql {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka
        Properties properties = KafkaUtil.buildKafkaProps();
        env
                .addSource(new FlinkKafkaConsumer<>(Constant.KAFKA_TOPIC_REAL_TIME, new SimpleStringSchema(), properties))
                .addSink(new MysqlSink());
        env.execute();
    }
}
