package com.librapi.xconsumer;



/**
 * 生产者线程
 * 
 * @author craig
 *
 */
public class Producer implements Runnable {

	private XConsumer crawlController;
	private URLFrontier frontier;

	public Producer() {
	}
	
	public void init(XConsumer crawlController) {
		this.crawlController = crawlController;
		frontier = crawlController.getFrontier();
	}
	
	public XConsumer getCrawlController() {
		return crawlController;
	}
	
	public URLFrontier getFrontier() {
		return frontier;
	}
	
	@Override
	public void run() {
		produce(frontier);
	}
	
	/*----------------*/
	protected void setup(Producer producer){
	}

	public void produce(URLFrontier frontier) {
	}
	
	
	
}
