package com.cvapp.mrshen;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class KafkaSink extends AbstractSink implements Configurable {

	private static final Log LOGGER = LogFactory.getLog(KafkaSink.class);
	private final String TOPIC = "testTopic";
	private Producer<String, String> producer;
	
	@Override
	public Status process() throws EventDeliveryException {
		// TODO Auto-generated method stub
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		
		try {
			transaction.begin();
			Event event = channel.take();
			if(event == null) {
				transaction.rollback();
				return Status.BACKOFF;
			}
			
//			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
//					TOPIC, new String(event.getBody()));
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(
					TOPIC, new String(event.getBody()));
			producer.send(data);
			transaction.commit();
			return Status.READY;
			
		} catch (Exception e) {
			// TODO: handle exception
			LOGGER.error("kafkasink exception", e);
			transaction.rollback();
			return Status.BACKOFF;
			
		} finally {
			transaction.close();
		}
	}

	@Override
	public void configure(Context arg0) {
		// TODO Auto-generated method stub
		Properties properties = new Properties();
//		properties.put("metadata.broker.list", "cvapp:9092");
//		properties.put("bootstrap.servers", "cvapp:9092");
		properties.put("bootstrap.servers", "slave3:9092,slave4:9092,slave5:9092");
		properties.put("acks", "1");
		properties.put("partitions.class", "example.producer.SimplePartitioner");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
//		properties.put("zookeeper.connect", "cvapp:2181");
		properties.put("zookeeper.connect", "master:2181,slave1:2181,slave2:2181");
		
		
		producer = new KafkaProducer<>(properties);
		LOGGER.info("***************kafkasink initial syccessfully.***************");
	}

}
