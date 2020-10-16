package org.example.etl.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.example.util.Constant;
import org.example.util.KafkaUtil;

import java.util.Collection;
import java.util.Properties;

/**
 * @Author: morris
 * @Date: 2020/10/16 16:18
 * @reviewer
 */
public class KafkaAgg2Mysql {
    public static void main(String[] args) {
        StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkStreamEnv.setParallelism(1);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv, blinkStreamSettings);
        Properties properties = KafkaUtil.buildKafkaProps();

        DataStream<String> dataStream = blinkStreamEnv.addSource(new FlinkKafkaConsumer<>(Constant.KAFKA_TOPIC_REAL_TIME, new SimpleStringSchema(), properties));


        //封装row type == spark中的fieldStruct
        TypeInformation<?>[] types = new TypeInformation<?>[11];
        for (int i = 0; i < 11; i++) {
            types[i]= Types.STRING;
        }
        String[] names = "deal_date,close_date,card_no,deal_value,deal_type,company_name,car_no,station,conn_mark,deal_money,equ_no".split(",");
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, names);
        SingleOutputStreamOperator<Row> map = dataStream.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                Collection<Object> values = jsonObject.values();
                Row row = new Row(values.size());
                Object[] objects = values.toArray();
                for (int i = 0; i < objects.length; i++) {
                    row.setField(i, objects[i]);
                }
                return row;
            }
        }).returns(rowTypeInfo);



        blinkStreamTableEnv.registerDataStream("zz",map);
        /*Table table = blinkStreamTableEnv.fromDataStream(dataStream,
                "deal_date,close_date,card_no,deal_value,deal_type,company_name,car_no,station,conn_mark,deal_money,equ_no");*/


//        blinkStreamTableEnv.registerTable("kafkaDataStream", table);
        Table table2 = blinkStreamTableEnv.sqlQuery("SELECT * FROM zz where  deal_value='0'");
        blinkStreamTableEnv.toRetractStream(table2, Row.class).print("########");
        try {
            blinkStreamEnv.execute("1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
