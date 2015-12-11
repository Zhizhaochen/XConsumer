package com.librapi.xconsumer;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;

/**
 * 直接使用已经建立好的http连接，提升吞吐率(不支持https，不支持代理，不支持登录)
 * @author craig
 *
 */
public class PageFetcher {

	private String userAgentString = "XConsumer";
	private int socketTimeout = 20000;
	private int connectionTimeout = 10000;
	private int maxTotalConnections = 100;
	private int maxConnectionsPerHost = 100;
	protected PoolingHttpClientConnectionManager connectionManager;
	protected CloseableHttpClient httpClient;
	protected final Object mutex = new Object();
	protected long lastFetchTime = 0;
	protected IdleConnectionMonitorThread connectionMonitorThread = null;
	private int POLITENESS_DELAY = 100;

	public PageFetcher(int POLITENESS_DELAY) {

		RequestConfig requestConfig = RequestConfig.custom()
				.setExpectContinueEnabled(false)
				.setCookieSpec(CookieSpecs.DEFAULT).setRedirectsEnabled(false)
				.setSocketTimeout(socketTimeout)
				.setConnectTimeout(connectionTimeout).build();

		RegistryBuilder<ConnectionSocketFactory> connRegistryBuilder = RegistryBuilder.create();
		connRegistryBuilder.register("http", PlainConnectionSocketFactory.INSTANCE);

		Registry<ConnectionSocketFactory> connRegistry = connRegistryBuilder.build();
		connectionManager = new PoolingHttpClientConnectionManager(connRegistry);
		connectionManager.setMaxTotal(maxTotalConnections);
		connectionManager.setDefaultMaxPerRoute(maxConnectionsPerHost);

		HttpClientBuilder clientBuilder = HttpClientBuilder.create();
		clientBuilder.setDefaultRequestConfig(requestConfig);
		clientBuilder.setConnectionManager(connectionManager);  //设置线程管理
		clientBuilder.setUserAgent(userAgentString);
		clientBuilder.setDefaultHeaders(new HashSet<BasicHeader>());

		httpClient = clientBuilder.build();

		if (connectionMonitorThread == null) {
			connectionMonitorThread = new IdleConnectionMonitorThread(connectionManager);
		}
		connectionMonitorThread.start();
		this.POLITENESS_DELAY = POLITENESS_DELAY;
	}

	public HttpEntity fetchPage(String toFetchURL) throws InterruptedException, IOException {
		HttpEntity httpEntity = null;
		HttpUriRequest request = null;
		try {
			request = newHttpUriRequest(toFetchURL);
			synchronized (mutex) { // 礼貌性
				long now = (new Date()).getTime();
				if ((now - lastFetchTime) < POLITENESS_DELAY) {
					Thread.sleep(POLITENESS_DELAY - (now - lastFetchTime));
				}
				lastFetchTime = (new Date()).getTime();
			}

			CloseableHttpResponse response = httpClient.execute(request);
			httpEntity = response.getEntity();
			return httpEntity;

		} finally {
			if ((httpEntity == null) && (request != null)) {
				request.abort();
			}
		}
	}

	public synchronized void shutDown() {
		if (connectionMonitorThread != null) {
			connectionManager.shutdown();
			connectionMonitorThread.shutdown();
		}
	}

	protected HttpUriRequest newHttpUriRequest(String url) {
		return new HttpGet(url);
	}

	private class IdleConnectionMonitorThread extends Thread {

		private final PoolingHttpClientConnectionManager connMgr;
		private volatile boolean shutdown;

		public IdleConnectionMonitorThread(
				PoolingHttpClientConnectionManager connMgr) {
			super("Connection Manager");
			this.connMgr = connMgr;
		}

		@Override
		public void run() {
			try {
				while (!shutdown) {
					synchronized (this) {
						wait(5000);
						connMgr.closeExpiredConnections();
						connMgr.closeIdleConnections(30, TimeUnit.SECONDS);
					}
				}
			} catch (InterruptedException ignored) {
			}
		}

		public void shutdown() {
			shutdown = true;
			synchronized (this) {
				notifyAll();
			}
		}
	}

}
