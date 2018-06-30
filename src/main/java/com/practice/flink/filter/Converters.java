package com.practice.flink.filter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.flink.model.ReceivedData;
import com.practice.flink.model.SensorData;

public class Converters{

public class JsonToReceiveData implements FlatMapFunction<String,ReceivedData>{

	@Override
	public void flatMap(String value, Collector<ReceivedData> out) throws Exception {
		ObjectMapper mapper=new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);
		ReceivedData data=mapper.readValue(value, ReceivedData.class);
		out.collect(data);
	}
}
	

public class JsonToSensorData implements FlatMapFunction<ReceivedData,SensorData>{
	@Override
	public void flatMap(ReceivedData value, Collector<SensorData> out) throws Exception {

		value.getData().forEach((k,v)->{
			SensorData sdata=new SensorData();
			sdata.setKey(k);
			sdata.setValue(new Double(v.toString()));
			sdata.setTimestamp(value.getTimestamp());
			out.collect(sdata);
		});
		
	}
}

}
