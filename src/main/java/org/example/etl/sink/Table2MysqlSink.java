package org.example.etl.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * @Author: morris
 * @Date: 2020/10/19 14:49
 * @reviewer
 */
public class Table2MysqlSink implements RetractStreamTableSink<Row> {

    private TableSchema tableSchema;

    public Table2MysqlSink(String[] fieldNames, TypeInformation[] typeInformations) {
        this.tableSchema = new TableSchema(fieldNames, typeInformations);
    }
    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
    }
    @Override
    public TableSchema getTableSchema() {
        return this.tableSchema;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {

    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        System.out.println(1111);
        return dataStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {

            }
        });
    }




    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return null;
    }
}
