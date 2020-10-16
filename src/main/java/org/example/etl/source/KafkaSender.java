package org.example.etl.source;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.model.Metric;
import org.example.util.Constant;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author: morris
 * @Date: 2020/10/14 10:39
 * @reviewer
 */
public class KafkaSender {
    public static final String BROKER_LIST = Constant.KAFKA_HOST;
    /** kafka topic，Flink 程序中需要和这个统一 */
    public static final String TOPIC = Constant.KAFKA_TOPIC;

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        //key 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            ProducerRecord record = new ProducerRecord<String, String>(TOPIC, null, null,SZTData.getData(i));
            producer.send(record);
            System.out.println("发送数据: " + SZTData.getData(i));
            producer.flush();
            Thread.sleep(10000);
        }

    }

    public static void main(String[] args) throws InterruptedException {
        while (true){
            writeToKafka();
//            Thread.sleep(1);
        }
    }
}
