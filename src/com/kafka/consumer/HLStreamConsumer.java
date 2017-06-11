package com.kafka.consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class HLStreamConsumer implements Runnable {

	private KafkaStream<byte[], byte[]> consumerStream;
	private int noOfThreads;

	public HLStreamConsumer(KafkaStream<byte[], byte[]> consumerStream, int noOfThreads) {
		this.noOfThreads = noOfThreads;
		this.consumerStream = consumerStream;
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = consumerStream.iterator();
		while (it.hasNext()){
			System.out.println("Thread " + noOfThreads + ": " + new String(it.next().message()));
		}
		System.out.println("Shutting down Thread: " + noOfThreads);
	}
}
