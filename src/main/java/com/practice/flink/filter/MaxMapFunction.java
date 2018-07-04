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

public class MaxMapFunction extends RichMapFunction<SensorData,Tuple3<String,String,Object>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient ValueState<Tuple1<Double>> max;
	

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<Tuple1<Double>> descriptor=new ValueStateDescriptor<>("max", TypeInformation.of(new TypeHint<Tuple1<Double>>() {}));
		max = getRuntimeContext().getState(descriptor);
		if(max==null) {
			max.update(Tuple1.of(0.0));
		}
	                        
	}


	@Override
	public Tuple3<String,String,Object> map(SensorData value) throws Exception {
	    if(max.value()==null)
	    	max.update(Tuple1.of(0.0));
		
		Tuple1<Double> currentSum = max.value();
        if(value.getValue()>=currentSum.f0) {
        	currentSum.f0=value.getValue();
        		max.update(currentSum);
        		return new Tuple3<>(value.getKey(),"max",currentSum.f0);
        }
		return null;
	}
	
	

}
