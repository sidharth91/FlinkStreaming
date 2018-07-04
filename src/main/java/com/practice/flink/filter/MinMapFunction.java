package com.practice.flink.filter;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import com.practice.flink.model.SensorData;

public class MinMapFunction extends RichMapFunction<SensorData,Tuple3<String,String,Object>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient ValueState<Tuple1<Double>> min;
	

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<Tuple1<Double>> descriptor=new ValueStateDescriptor<>("min", TypeInformation.of(new TypeHint<Tuple1<Double>>() {}));
		min = getRuntimeContext().getState(descriptor);
		if(min==null) {
			min.update(Tuple1.of(0.0));
		}
	                        
	}


	@Override
	public Tuple3<String,String,Object> map(SensorData value) throws Exception {
	    if(min.value()==null)
	    	min.update(Tuple1.of(0.0));
		
		Tuple1<Double> currentSum = min.value();
        if(currentSum.f0==0.0 || value.getValue()<=currentSum.f0) {
        		currentSum.f0=value.getValue();
        		min.update(currentSum);
        		return new Tuple3<>(value.getKey(),"min",currentSum.f0);
        }
		return null;
	}
	
	

}
