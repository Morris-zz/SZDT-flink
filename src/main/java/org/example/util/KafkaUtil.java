package org.example.util;

import java.util.Properties;

/**
 * @Author: morris
 * @Date: 2020/10/16 16:31
 * @reviewer
 */
public class KafkaUtil {
    public static Properties buildKafkaProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFKA_HOST);


        return props;
    }
}
