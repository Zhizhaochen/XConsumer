package com.librapi.xconsumer;

import java.util.ArrayList;
import java.util.List;

/**
 * 消费者线程
 * @author craig
 *
 */
public class Consumer implements Runnable {

	//protected ConsumeContext context;
	protected int consumerId;
	public boolean isWaitingForNewURLs;
	public Thread myThread;
	protected XConsumer myController;
	public URLFrontier frontier;
	private PageFetcher pageFetcher;
	
	public Consumer() {}
	
	protected void init(int consumerId, XConsumer crawlController) {
		this.consumerId = consumerId;
		this.isWaitingForNewURLs = false;
		this.myController = crawlController;
		this.frontier = crawlController.getFrontier();
		this.pageFetcher = crawlController.getPageFetcher();
		//bw = Files.newWriter(new File(dataDir + "/crawler-" + consumerId + ".data"), Charsets.UTF_8);
	}

	protected boolean setup(Consumer consumer) {
		return true;
	}
	protected void consume(Consumer consumer, String cURL) {
	}
	/**
	 * Do something before current thread exists.
	 * @param consumer
	 */
	protected void onBeforeExist(Consumer consumer) {
	}
	
	@Override
	public void run() {
		// System.out.println("frontier: run");
		if(setup(this)){
			FLAG : while (true) {
				// System.out.println("\tfrontier: while true!");
				List<String> assignedURLs = new ArrayList<>(myController.getMaxItemsFrontierOnceFetch());
				isWaitingForNewURLs = true;
				// System.out.println("\t"+consumerId+" frontier: 1");
				frontier.getNextURLs(myController.getMaxItemsFrontierOnceFetch(), assignedURLs);
				isWaitingForNewURLs = false;
				
				// System.out.println("\t"+consumerId+"\t frontier: 2");
				// System.out.println("\t"+consumerId+"frontier: " + assignedURLs.isEmpty());
				System.out.println(consumerId + " fetched: " + assignedURLs.size());
				if (assignedURLs.isEmpty()) {
					
					// System.out.println("frontier: " + frontier.isFinished());
					if (frontier.isFinished()) {
						System.out.println("frontier isFinished! " + consumerId + " return!" );
						break;
					}
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						System.out.println("Error occurred:" + e);
					}
				} else {
					for (String curURL : assignedURLs) {
						if (curURL != null) {
							consume(this, curURL);
						}
						if (myController.shuttingDown) {
							System.out.println("Exiting because of controller shutdown.");
							break FLAG;
						}
					}
				}
			}
			onBeforeExist(this);
		}
		
	}

	public void setThread(Thread myThread) {
		this.myThread = myThread;
	}

	public PageFetcher getPageFetcher() {
		return pageFetcher;
	}
	public int getConsumerId() {
		return consumerId;
	}
	
}
