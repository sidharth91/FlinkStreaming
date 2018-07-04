package com.practice.flink.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import redis.clients.jedis.Jedis;

public class RedisAggregateFlinkSink extends RichSinkFunction<Tuple3<String,String,Object>> {

	private String host;
	private int port;
	private Jedis jedis;
	
public RedisAggregateFlinkSink(String host,Integer port) {
		this.host=host;
		this.port=port;
		
	}

@Override
	public void open(Configuration parameters) throws Exception {
	this.jedis = new Jedis(this.host, this.port);	
	}
	
@Override
public void invoke(Tuple3<String, String, Object> value, Context context) throws Exception {
	try {
	jedis.hset(value.f0, value.f1,value.f2.toString());
	}catch (Exception e) {
		System.err.println(e.getMessage());// TODO: handle exception
	}
}
}
