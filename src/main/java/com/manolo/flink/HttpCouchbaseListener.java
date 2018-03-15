package com.manolo.flink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class HttpCouchbaseListener {
	
	private LinkedBlockingQueue<String> queue;
	private transient HttpServer server;
	
	private static HttpCouchbaseListener instance = null;

	public static void main(String[] args) throws Exception {
		startServer(Integer.parseInt(args[0]), args[1], new LinkedBlockingQueue<String>(10000));
	}
	
	public static void startServer(int port, String context, LinkedBlockingQueue<String> queue) {
		if(instance == null) {
			instance = new HttpCouchbaseListener(port, context, queue);
			instance.start();
		}
	}
	
	public static void stopServer() {
		if(instance != null) {
			instance.stop();
		}
	}
	
	private HttpCouchbaseListener(int port, String context, LinkedBlockingQueue<String> queue) {
		try {
			this.queue = queue;
			System.out.println(String.format("=== HttpCouchbaseListener === Starting http server on port: %d, contextUri: %s", port, context));
			server = HttpServer.create(new InetSocketAddress(port), 0);
			server.createContext(context, new ReqHandler(queue));
			server.setExecutor(null);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	private void start() {
		server.start();		
	}
	
	private void stop() {
		server.stop(30);	
	}
	
	static class ReqHandler implements HttpHandler {
		LinkedBlockingQueue<String> queue;
		
		public ReqHandler(LinkedBlockingQueue<String> queue) {
			super();
			this.queue = queue;
		}
		
		@Override
		public void handle(HttpExchange t) throws IOException {
			String response = "{\"response\":\"OK\"}";
			String body = StringFromBody(t.getRequestBody());
			boolean inserted = queue.offer(body);
			if(!inserted) {
				System.out.println("Warning: queue full");
			}
			t.sendResponseHeaders(200, response.length());
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();
		}
	}

	private static String StringFromBody(InputStream input) {
		String res = null;
		try {
			ByteArrayOutputStream result = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			int length;
			while ((length = input.read(buffer)) != -1) {
				result.write(buffer, 0, length);
			}
			res = result.toString("UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}

}
