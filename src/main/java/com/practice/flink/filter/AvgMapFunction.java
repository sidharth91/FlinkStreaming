package com.practice.flink.filter;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import com.practice.flink.model.SensorData;

public class AvgMapFunction extends RichMapFunction<SensorData,SensorData>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient ValueState<Tuple2<Double, Double>> sum;
	

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<Tuple2<Double,Double>> descriptor=new ValueStateDescriptor<>("average", TypeInformation.of(new TypeHint<Tuple2<Double, Double>>() {}));
		sum = getRuntimeContext().getState(descriptor);
		if(sum==null) {
			sum.update(Tuple2.of(0.0, 0.0));
		}
	                        
	}


	@Override
	public SensorData map(SensorData value) throws Exception {
	    if(sum.value()==null)
	    	sum.update(Tuple2.of(0.0, 0.0));
		
		Tuple2<Double, Double> currentSum = sum.value();
        currentSum.f0 += 1;
        currentSum.f1 += value.getValue();
        sum.update(currentSum);
        value.setValue(currentSum.f1 / currentSum.f0);
        return value;
	}
	
	

}
