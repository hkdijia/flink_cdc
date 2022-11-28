package com.gotk.app;

import com.alibaba.fastjson.JSONObject;


import com.gotk.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;


public class Flink_CDCWithCustomerSchema {
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        prop.setProperty("useSSL","false");
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        //2.创建 Flink-MySQL-CDC 的 Source
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
            .hostname("hadoop102").port(3306).username("root").password("123456")
            .databaseList("hk")
                .tableList("hk.user") // set captured table
            .startupOptions(StartupOptions.latest())
            .jdbcProperties(prop)
            .deserializer(new DebeziumDeserializationSchema<String>() {
                //自定义数据解析器
                @Override
                public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
                    // 获 取 主 题 信 息 , 包 含 着 数 据 库 和 表 名 mysql_binlog_source.gmall-flink.z_user_info
                    String topic = sourceRecord.topic();
                    String[] arr = topic.split("\\.");
                    String db = arr[1];
                    String tableName = arr[2];
                    //获取操作类型 READ DELETE UPDATE CREATE
                    Envelope.Operation operation = Envelope.operationFor(sourceRecord);
                    //获取值信息并转换为 Struct 类型
                    Struct value = (Struct) sourceRecord.value();
                    //获取变化后的数据
                    Struct after = value.getStruct("after");
                    //创建 JSON 对象用于存储数据信息
                    JSONObject data = new JSONObject();
                    if (after != null) {
                        Schema schema = after.schema();
                        for (Field field : schema.fields()) {
                            data.put(field.name(), after.get(field.name()));
                        }
                    }
                    //创建 JSON 对象用于封装最终返回值数据信息
                    JSONObject result = new JSONObject();
                    result.put("operation", operation.toString().toLowerCase());
                    result.put("data", data);
                    result.put("database", db);
                    result.put("table", tableName);
                    //发送数据至下游
                    collector.collect(result.toJSONString());
                }
                
                @Override
                public TypeInformation<String> getProducedType() {
                    return TypeInformation.of(String.class);
                }
            }).build();


        //3.使用 CDC Source 从 MySQL 读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(
                mysqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL Source");
        //4.打印数据
        mysqlDS.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));

        //5.执行任务
        env.execute();
    } 
}