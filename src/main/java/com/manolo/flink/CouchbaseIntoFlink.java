package com.manolo.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CouchbaseIntoFlink {

    public static void main(String[] args) throws Exception {

        final int port;
        final String contextUri;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
            contextUri = params.get("contextUri");
        } catch (Exception e) {
            System.err.println("Please run 'CouchbaseSource --port <port> --contextUri <httpContextUri> '");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        	env.addSource(new HttpCouchbaseSource(port, contextUri))
        		.name("Couchbase Source Streaming")
        		.print()
        		.name("stdout")
        		.setParallelism(1);
        env.execute("Ingest data from Couchbase into Flink");
    }
    
}
