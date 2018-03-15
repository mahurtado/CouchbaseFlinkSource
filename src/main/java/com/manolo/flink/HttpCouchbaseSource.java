package com.manolo.flink;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class HttpCouchbaseSource implements SourceFunction<String> {
	
	private int port;
	private String contextUri;
	private boolean active;
	private LinkedBlockingQueue<String> queue;
	
	public static final int QUEUE_SIZE = 10000;
	public static final long DELAY = 1000l;

	public HttpCouchbaseSource(int port, String contextUri) {
		super();
		this.port = port;
		this.contextUri = contextUri;
		this.active = true;
		this.queue = new LinkedBlockingQueue<String>(QUEUE_SIZE);
	}

	@Override
	public void cancel() {
		HttpCouchbaseListener.stopServer();
		active = false;
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		HttpCouchbaseListener.startServer(port, contextUri, queue);
		System.out.println("=== HttpCouchbaseStream === Listening for messages ....");
		while(active) {
			if(queue.isEmpty()) {
				Thread.sleep(DELAY);
				continue;
			}
			String msg = queue.poll();
			ctx.collect(msg);
        }
	}

}
