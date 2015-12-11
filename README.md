# XConsumer
A java Producer/Multi-Consumer tool based on BlockingQueue.

# Usage:

```java
public class Main {
	
	public static class IProducer extends Producer {
		@Override
		public void produce(URLFrontier frontier) {
			//Do something ...
		}
	}
	
	public static class IConsumer extends Consumer {
		@Override
		protected void consume(Consumer consumer, String curURL) {
			//Do something ...
		}
	}
	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException {
		
		XConsumer xConsumer = new XConsumer.Builder()
			.setProduceClass(IProducer.class)
			.setConsumeClass(IConsumer.class)
			.setMaxItemsFrontierOnceFetch(30)
			.setNCrawlers(7)
			.build();
		xConsumer.start();
	}
}
```
