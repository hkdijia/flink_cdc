package com.gotk.app;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
 
import java.util.Properties;
 
public class FlinkCDCTest {
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        prop.setProperty("useSSL","false");
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("hadoop102")
//                .port(3306)
//                .databaseList("test") // set captured database
//                .tableList("test.user") // set captured table
//                .username("root")
//                .password("123456")
//                .serverTimeZone("UTC")
//                .startupOptions(StartupOptions.latest())
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                .jdbcProperties(prop)
//                .startupOptions(StartupOptions.initial())
//                .build();
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//     // enable checkpoint
//     env.enableCheckpointing(10000);
//     env
//       .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//       .setParallelism(1)
//       .print().setParallelism(1);
//
//     env.execute("mysql-cdc");


//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("rm-2zek85dzv7g624hbz.mysql.rds.aliyuncs.com")
//                .port(3306)
//                .databaseList("fin-loan-risk") // set captured database
//                .tableList("fin-loan-risk.feature_query_log") // set captured table
//                .startupOptions(StartupOptions.latest())
//                .username("fintest")
//                .password("Zhnx#Capp")
//                .jdbcProperties(prop)
//                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .build();

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("hk") // set captured database
                .tableList("hk.user") // set captured table
                .startupOptions(StartupOptions.latest())
                .username("root")
                .password("123456")
                .jdbcProperties(prop)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env
        .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
        // set 4 parallel source tasks
        .print(); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");

    }
}