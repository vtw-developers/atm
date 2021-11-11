package com.example.atm;

import org.apache.camel.builder.endpoint.EndpointRouteBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

@Component
public class HdfsProducerRoute extends EndpointRouteBuilder {

    @Override
    public void configure() throws Exception {
        from(timer("test").repeatCount(1))
        .log("HdfsProducerRoute Started")
        .setBody().constant("select code_name, count(*), max(close_price), min(close_price) from stock group by code_name")

                .process(ex -> {
                    // HDFS 설정
                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://taejin-1:9000");
                    conf.set("dfs.client.use.datanode.hostname", "true");
//                    conf.set("dfs.client.use.datanode.hostname", "true");
//                    conf.set("dfs.nameservices", "mycluster");
//                    conf.set("dfs.ha.namenodes.mycluster", "cluster_n1");
//                    conf.set("dfs.namenode.rpc-address.mycluster.cluster_n1", "/taejin-1:9000");
//                    conf.set("dfs.client.failover.proxy.provider.mycluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

// FileSystem 설정
                    FileSystem dfs = FileSystem.get(conf);
                    Path filenamePath = new Path("/test2/test2.txt");
                    System.out.println("File Exists : " + dfs.exists(filenamePath));

// Write data
                    FSDataOutputStream out = dfs.create(filenamePath);
                    out.write("TEST".getBytes());
                    out.close();

// Close FileSystem
                    dfs.close();
                })




        // .to(hdfs("34.64.224.104:9000/test1/abc.csv").overwrite(true))
        //.to(hdfs("aaa/test1").namedNodes("taejin-1:9000"))
//        .process(exchange -> {
//            Class<?> aClass = Class.forName("org.apache.hive.jdbc.HiveDriver");
//            Connection cnct = DriverManager.getConnection("jdbc:hive2://34.64.224.104:10000/default", "hadoop", "30539wDbsAl1!");
//
//            Statement stmt = cnct.createStatement();
//            stmt.execute("insert into stock values ('20200105', 'SAM', 62000)");
////            ResultSet rs = stmt.executeQuery("select code_name, count(*), max(close_price), min(close_price) from stock group by code_name");
////            // Fetch each row from the result set
////            while (rs.next()) {
////                // Get the data from the row using the column index
////                String s = rs.getString(1);
////                log.info(s);
////                s = rs.getString(2);
////                log.info(s);
////                s = rs.getString(3);
////                log.info(s);
////            }
//        })
        //.to(jdbc("dataSource"))
        //.to(sql("select code_name, count(*), max(close_price), min(close_price) from stock group by code_name"))
        .log("HdfsProducerRoute Completed");
    }
}
