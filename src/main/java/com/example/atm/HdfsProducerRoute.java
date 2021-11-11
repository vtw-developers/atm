package com.example.atm;

import org.apache.camel.builder.endpoint.EndpointRouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class HdfsProducerRoute extends EndpointRouteBuilder {

    @Override
    public void configure() throws Exception {
        from(timer("10s"))
        .log("HdfsProducerRoute Started")
        .setBody().constant("test1")
        .to(jdbc("dataSource"))
        .log("HdfsProducerRoute Completed");
    }
}
