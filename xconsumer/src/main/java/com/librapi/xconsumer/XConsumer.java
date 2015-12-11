package com.librapi.xconsumer;

import java.util.ArrayList;
import java.util.List;

public class XConsumer {
	
	private Class<? extends Producer> produceClass;
	private Class<? extends Consumer> consumeClass;

	private URLFrontier frontier;
	private PageFetcher pageFetcher;
	private boolean finished;
	protected boolean shuttingDown;
	private final Object waitingLock = new Object();
	private int nCrawlers;
	private int politenessDelay;
	private int maxItemsFrontierOnceFetch;
	//private int MAX_QUEUE_LENGTH = 1000;
	
	/**
	   * Build a new {@link XConsumer}.
	   * <p>
	   * Calling {@link #setProduceClass}, {@link #setConsumeClass}is required before calling {@link #build()}. All other methods
	   * are optional.
	   */
	public static final class Builder {
		private Class<? extends Producer> produceClass;
		private Class<? extends Consumer> consumeClass;
		
		private int nCrawlers = 7;
		private int politenessDelay = 1000;
		private int maxItemsFrontierOnceFetch = 50;
		
		public Builder() {}
		
		/** Create the {@link XConsumer} instances. */
	    public XConsumer build() {
	    	XConsumer xConsumer = new XConsumer(produceClass, consumeClass);
	    	xConsumer.politenessDelay = politenessDelay;
	    	xConsumer.nCrawlers = nCrawlers;
	    	xConsumer.maxItemsFrontierOnceFetch = maxItemsFrontierOnceFetch;
			return xConsumer;
	    }
		public Builder setProduceClass(Class<? extends Producer> produceClass) {
			this.produceClass = produceClass;
			return this;
		}
		public Builder setConsumeClass(Class<? extends Consumer> consumeClass) {
			this.consumeClass = consumeClass;
			return this;
		}
		
		public Builder setMaxItemsFrontierOnceFetch(int maxItemsFrontierOnceFetch) {
			if(maxItemsFrontierOnceFetch > 0){
				this.maxItemsFrontierOnceFetch = maxItemsFrontierOnceFetch;
			}else {
				throw new IllegalArgumentException("maxItemsFrontierOnceFetch should large than 0.");
			}
			return this;
		}
		
		/**
		 * 每一次访问时间间隔，默认1000ms
		 * @param politenessDelay
		 */
		public Builder setPolitenessDelay(int politenessDelay) {
			if(politenessDelay > 0){
				this.politenessDelay = politenessDelay;
			}else {
				throw new IllegalArgumentException("politenessDelay should large than 0.");
			}
			return this;
		}
		
		/**
		 * 消费者线程并行数量
		 * @param nCrawlers
		 * @return
		 */
		public Builder setNCrawlers(int nCrawlers) {
			if(nCrawlers > 0){
				this.nCrawlers = nCrawlers;
			}else {
				throw new IllegalArgumentException("nCrawlers should large than 0.");
			}
			return this;
		}
	}
	
	/* --------------------------------- Construction Method ----------------------------------- */
	public XConsumer (Class<? extends Producer> produceClass, Class<? extends Consumer> consumeClass) {
		this.consumeClass = consumeClass;
		this.produceClass = produceClass;
		frontier = new URLFrontier();
		pageFetcher = new PageFetcher(politenessDelay);
		finished = false;
		shuttingDown = false;
	}
	
	/**
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * 
	 */
	public void start() throws InstantiationException, IllegalAccessException {
		
		startThreads();
		while (!finished) {  //Blocking....
			synchronized (waitingLock) {
				if (finished) {
					System.out.println("-----finished!");
					return;
				}
				try {
					waitingLock.wait();
				} catch (InterruptedException e) {
					System.out.println("Error occurred");
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * 
	 */
	public void startThreads() throws InstantiationException, IllegalAccessException {
		
		// 启动生产者线程
		Producer producer = produceClass.newInstance();
		producer.init(this);
		new Thread(producer, "Producer").start();
		System.out.println("Producer started");
		
		// 启动消费者线程
		finished = false;
		final List<Thread> threads = new ArrayList<>();
		final List<Consumer> crawlers = new ArrayList<>();

		for (int i = 1; i <= nCrawlers; i++) {
			//Consumer crawler = new Consumer();
			Consumer crawler = consumeClass.newInstance();
			Thread thread = new Thread(crawler, "Crawler " + i);
			crawler.setThread(thread);
			crawler.init(i, this);
			thread.start();
			crawlers.add(crawler);
			threads.add(thread);
			System.out.println("Consumer " + i + " started");
		}

		// 启动监控线程
		final XConsumer controller = this;
		Thread monitorThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					synchronized (waitingLock) {

						while (true) {
							System.out.println("----> Daemon thread started,  Quene:" + frontier.getQueueLength());
							sleep(10);
							boolean someoneIsWorking = false;
							for (int i = 0; i < threads.size(); i++) {
								Thread thread = threads.get(i);
								//System.out.println("Consumer " + i + " " + crawlers.get(i).isNotWaitingForNewURLs());
								if (!thread.isAlive()) {
									System.out.println("thread is not live!");
									if (!shuttingDown) {
										System.out.println("Thread " + i + " was dead, I'll recreate it");
										Consumer crawler = consumeClass.newInstance();
										thread = new Thread(crawler, "Crawler " + (i + 1));
										threads.remove(i);
										threads.add(i, thread);
										crawler.setThread(thread);
										crawler.init(i + 1, controller);
										thread.start();
										crawlers.remove(i);
										crawlers.add(i, crawler);
									}
									//!isWaitingForNewURLs
								} else {
									someoneIsWorking = !crawlers.get(i).isWaitingForNewURLs;
								}
							}

							if (!someoneIsWorking) {
								//需要再次确认
								System.out.println("----> 1. It looks like no thread is working, waiting for 10 seconds to make sure...");
								sleep(10);

								someoneIsWorking = false;
								for (int i = 0; i < threads.size(); i++) {
									//System.out.println(i + " " + crawlers.get(i).isNotWaitingForNewURLs());
									someoneIsWorking = threads.get(i).isAlive() && !crawlers.get(i).isWaitingForNewURLs;
								}
								
								if (!someoneIsWorking) {
									if (!shuttingDown) {
										long queueLength = frontier.getQueueLength();
										if (queueLength > 0) {
											continue;
										}
										System.out.println("----> 2. No thread is working and no more URLs are in queue waiting for another 10 seconds to make sure...");
										sleep(10);
										queueLength = frontier.getQueueLength();
										if (queueLength > 0) {
											continue;
										}
									}

									System.out.println("----> 3. All of the crawlers are stopped. Finishing the process...");
									frontier.finish();

									System.out.println("----> 4. Waiting for 10 seconds before final clean up...");
									sleep(10);

									finished = true;
									pageFetcher.shutDown();
									waitingLock.notifyAll();
									return;
								}
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		monitorThread.start();
		
	}

	protected static void sleep(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException ignored) {
		}
	}
	
	/**
	 * 手动关闭，一般不需要
	 */
	public void shutdown() {
		System.out.println("Shutting down...");
		this.shuttingDown = true;
		pageFetcher.shutDown();
		frontier.finish();
	}
	
	/************************************** SETTING **************************************/
	
	public PageFetcher getPageFetcher() {
		return pageFetcher;
	}
	public URLFrontier getFrontier() {
		return frontier;
	}

	/**
	 * 每一个线程一次从前端队列中取URL的最大条数
	 * @return
	 */
	public int getMaxItemsFrontierOnceFetch() {
		return this.maxItemsFrontierOnceFetch;
	}
	
}
