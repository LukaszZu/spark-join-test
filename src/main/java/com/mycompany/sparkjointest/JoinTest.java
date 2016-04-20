/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sparkjointest;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

/**
 *
 * @author zulk
 */
public class JoinTest {

    private SparkConf sparkConf;
    private JavaSparkContext sparkContext;
    private SQLContext sqlContext;

    public void encodeTest() {
        DataFrame df1 = sqlContext.read()
                .options(pgAnalytics())
                .format("jdbc")
                .load();
        JavaRDD<Row> javaRDD = df1.javaRDD();

        javaRDD.checkpoint();
        javaRDD.count();
        //                .as(Encoders.bean(Analytics.class));

//        df1.map(s -> {
//            return s;
//        });
        df1.printSchema();

        JavaRDD<Row> map = javaRDD.map(f -> RowFactory.create("Aaa", "bb"));
        map.checkpoint();
        map.count();

        String[] jarOfClass = JavaSparkContext.jarOfClass(JavaRDD.class);

        Arrays.stream(jarOfClass)
                .forEach(System.out::println);

        System.out.println(map.isCheckpointed());
        System.out.println(df1.javaRDD()
                .toDebugString());

//        df1.
        if (true) {
//            throw new RuntimeException("aaa");
        }

        System.out.println(df1.javaRDD()
                .partitioner());

        JavaRDD<Analytics> map1;
//        CassandraJavaUtil.javaFunctions(df1.javaRDD()).
//        df1.withColumnRenamed("date", "data")
//                .write()
//                .format("org.apache.spark.sql.cassandra")
//                .option("", "")
//                .mode(SaveMode.Overwrite)
//                .options(ImmutableMap
//                        .of("table", "analytics", "keyspace", "kkk"))
//                .save();//        CassandraJavaUtil.javaFunctions(df1.)
//                //                .writerBuilder("kkk", "analytics",
        //                        CassandraJavaUtil.mapToRow(Analytics.class, ImmutableMap
        //                                .of("date", "data")))
        //                .saveToCassandra();
        map1 = df1.toJavaRDD()
                .map(row -> {
                    Analytics analytics = new Analytics();
                    analytics.setId(row.getAs("id"));
                    analytics.setName(row.getAs("name"));
            analytics.setEndtime(row.getAs("endtime"));
            analytics.setDate(row.getAs("date"));
            analytics.setName3(ImmutableSet.of("AAA", "BBB"));
                    return analytics;
                }
                );

        CassandraJavaUtil.javaFunctions(map1)
                .writerBuilder(
                        "kkk", "anal", CassandraJavaUtil
                        .mapToRow(Analytics.class, mapFields()))
                .withAutoTTL()
                .withColumnSelector(CassandraJavaUtil
                        .someColumns("id", "data", "endtime", "name", "name3"))
                .withIfNotExists(true)
                .withIgnoreNulls(true)
                .saveToCassandra();

    }

    Map<String, String> mapFields() {
        return ImmutableMap
                .of("date", "data", "endTime", "endtime");
    }

    public void jointest() {
        Dataset<D1> df1 = sqlContext.read()
                .options(pg1(1000))
                .format("jdbc")
                .load()
                .as(Encoders.bean(D1.class));

        Dataset<D2> df2 = sqlContext.read()
                .options(pg2())
                .format("jdbc")
                .load()
                .as(Encoders.bean(D2.class));

//        df1.
//        df2.registerTempTable("d2");

        df1.printSchema();
        df2.printSchema();

        DataFrame sql = sqlContext
                .sql("select count(1) from d1 join d2 on d1.n1 = d2.id where d1.n1 < 100");

//        long count = sql.count();
        List<Row> collectAsList = sql.collectAsList();
        System.out.println(collectAsList.size());

    }

    public void config() {
        String logFile = "/var/log/*.log"; // Should be some file on your system
        //                .setMaster("spark://192.168.100.105:7077")

        sparkConf = new SparkConf()
                .setAppName("Simple Application")
//                .setMaster("spark://192.168.100.105:7077")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", "127.0.0.1")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setJars(new String[]{"/home/zulk/bin/javalib/postgresql-9.4.1207.jar"});
        sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setCheckpointDir("/tmp/spark-chk");
//        sparkContext.checkpointFile(logFile)

        sqlContext = new SQLContext(sparkContext);
        sqlContext.setConf("spark.sql.caseSensitive", "false");
//
    }

    public Map<String, String> pg1(int testMax) {
        Map<String, String> options = new HashMap<String, String>();
        options
                .put("url", "jdbc:postgresql://192.168.100.105:5432/ala?user=zulk&loglevel=2");
        options.put("dbtable", "test");
        options.put("driver", "org.postgresql.Driver");
        options.put("partitionColumn", "n1");
        options.put("lowerBound", "0");
//        options.put("spark.sql.autoBroadcastJoinThreshold","50485760");
        options.put("upperBound", Integer.toString(testMax));
        options.put("numPartitions", "8");
        return options;
    }

    public Map<String, String> pg2() {
        Map<String, String> options1 = new HashMap<String, String>();
        options1
                .put("url", "jdbc:postgresql://192.168.100.105:5432/ala?user=zulk&loglevel=2");
        options1.put("dbtable", "test1");
        options1.put("driver", "org.postgresql.Driver");
        options1.put("partitionColumn", "id");
        options1.put("lowerBound", "0");
        options1.put("upperBound", "1000");
        options1.put("numPartitions", "8");
        return options1;
    }

    public Map<String, String> pgAnalytics() {
        Map<String, String> options1 = new HashMap<String, String>();
        options1
                .put("url", "jdbc:postgresql://192.168.100.105:5432/ala?user=zulk&loglevel=2");
        options1.put("dbtable", "Analytics");
        options1.put("driver", "org.postgresql.Driver");
        return options1;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        JoinTest jdbcSparkTest = new JoinTest();
        jdbcSparkTest.config();
//        jdbcSparkTest.jointest();
//        jdbcSparkTest.spark();
        jdbcSparkTest.encodeTest();
//        System.in.read();
    }

}
