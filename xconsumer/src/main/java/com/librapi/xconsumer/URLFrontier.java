package com.librapi.xconsumer;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 前端队列
 * 
 * @author craig
 *
 */
public class URLFrontier {

	protected final Object mutex = new Object();
	protected final Object waitingList = new Object();
	protected boolean isFinished = false;
	private BloomFilter<String> bf = new BloomFilter<String>(0.1, 1000000);
	private LinkedBlockingQueue<String> workQueues = new LinkedBlockingQueue<String>();

	/**
	 * 取
	 * @param max
	 * @param result
	 */
	public void getNextURLs(int max, List<String> result) {
		while (true) {
			synchronized (mutex) {
				if (isFinished) {
					return;
				}
				
				for (int idx = 0; idx < max; idx++) {
					String nextURL = workQueues.poll();
					if (nextURL != null) {
						result.add(nextURL);
					} else {
						break;
					}
				}

				if (result.size() > 0) {
					return;
				}else {
					System.out.println("Nothing fetch!");
				}
			}

			try {
				synchronized (waitingList) {
					System.out.println("等待URL到来....");
					waitingList.wait();  //置为等待状态
				}
			} catch (InterruptedException ignored) {
			}
			if (isFinished) {
				return;
			}
		}
	}

	/**
	 * 批量增加元素
	 * @param urls
	 */
	public void scheduleAll(List<String> urls) {
		
		for (String url : urls) {
			try {
				if(!bf.contains(url)){
					workQueues.put(url);
					bf.add(url);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		synchronized (mutex) {
			synchronized (waitingList) {
				waitingList.notifyAll();
			}
		}
	}

	@Deprecated
	public void schedule(String url) {
		//synchronized (mutex) {
			try {
				workQueues.put(url);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		//}
	}

	public long getQueueLength() {
		return workQueues.size();
	}

	public boolean isFinished() {
		return isFinished;
	}

	public void finish() {
		isFinished = true;
		synchronized (waitingList) {
			waitingList.notifyAll();
		}
	}
	
}
