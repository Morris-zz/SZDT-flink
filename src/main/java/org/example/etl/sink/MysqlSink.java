package org.example.etl.sink;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @Author: morris
 * @Date: 2020/10/19 14:58
 * @reviewer
 */
@Slf4j
public class MysqlSink extends RichSinkFunction<String> {

    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "insert into station(card_no, station) values(?, ?);";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {

        JSONObject jsonObject = JSONObject.parseObject(value);
        if (ps != null) {
            ps.setString(1, jsonObject.getString("card_no"));
            ps.setString(2,jsonObject.getString("station"));
            log.info(jsonObject.getString("station"));
            ps.addBatch();
            int[] count = ps.executeBatch();//批量后执行
            log.info("成功了插入了 {} 行数据" + count.length);
        }



    }
    private static Connection getConnection (BasicDataSource dataSource){
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl("jdbc:mysql://hadoop001:3306/test");
        dataSource.setUsername("root");
        dataSource.setPassword("");
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            log.info("创建连接池：{}" + con);
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = {}" + e.getMessage());
        }
        return con;
    }
}
