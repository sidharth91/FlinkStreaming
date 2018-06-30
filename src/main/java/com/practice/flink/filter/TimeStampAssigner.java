package com.practice.flink.filter;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.practice.flink.model.ReceivedData;

public class TimeStampAssigner implements AssignerWithPunctuatedWatermarks<ReceivedData> {

	@Override
	public long extractTimestamp(ReceivedData element, long previousElementTimestamp) {
		return element.getTimestamp();
	}

	@Override
	public Watermark checkAndGetNextWatermark(ReceivedData lastElement, long extractedTimestamp) {
		// TODO Auto-generated method stub
		return null;
	}




}
