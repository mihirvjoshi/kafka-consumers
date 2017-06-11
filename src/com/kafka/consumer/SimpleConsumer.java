package com.kafka.consumer;

import java.util.Arrays;

import kafka.consumer.ConsumerIterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleConsumer implements Runnable {

	private KafkaConsumer<String, String> consumer;
	private int noOfThreads;
	private String topic;

	public SimpleConsumer(KafkaConsumer<String, String> consumer, int noOfThreads, String topic) {
		this.noOfThreads = noOfThreads;
		this.consumer = consumer;
		this.topic = topic;
	}
	
	@Override
	public void run() {
		consumer.subscribe(Arrays.asList(topic));
	    System.out.println("Subscribed to topic " + topic);
	    int i = 0;
	    while (true) {
	       ConsumerRecords<String, String> records = consumer.poll(100);
	          for (ConsumerRecord<String, String> record : records){
	  			 System.out.println("Thread " + noOfThreads + ": " + new String(record.value()));
	  			 System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
	          }
	  		System.out.println("Shutting down Thread: " + noOfThreads);
	    }     
	}

}
