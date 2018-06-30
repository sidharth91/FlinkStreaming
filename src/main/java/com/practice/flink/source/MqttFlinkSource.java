package com.practice.flink.source;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttFlinkSource extends RichSourceFunction<MqttMessage> implements MqttCallback {

	private MqttClient client;
	private String topic;
	private String clientid;
	private String broker;
	private Queue<MqttMessage> queue;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		this.client = new MqttClient(broker, clientid);
		 MqttConnectOptions connOpts = new MqttConnectOptions();
		 connOpts.setUserName("sid");
		 connOpts.setPassword("sidharth".toCharArray());
         connOpts.setCleanSession(true);
         this.client.connect(connOpts);
         this.client.subscribe(topic);
         this.client.setCallback(this);
         queue=new LinkedBlockingQueue<>();
	}
	
	public MqttFlinkSource(String broker, String clientID, String topic) throws MqttException {
		this.clientid=clientID;
		this.broker=broker;
         this.topic=topic;
	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void run(SourceContext<MqttMessage> context) throws Exception {
	
		while(true) {
			if(!queue.isEmpty())
			context.collect(queue.remove());
		}
		
	}

	@Override
	public void connectionLost(Throwable arg0) {
		try {
			client.close();
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		queue.add(message);
		
	}
	

}
