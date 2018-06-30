package com.practice.flink.model;

public class SensorData {
private String key;
private Double value;
private Long timestamp;
public String getKey() {
	return key;
}
public void setKey(String key) {
	this.key = key;
}


public Double getValue() {
	return value;
}
public void setValue(Double value) {
	this.value = value;
}
public Long getTimestamp() {
	return timestamp;
}
public void setTimestamp(Long timestamp) {
	this.timestamp = timestamp;
}
@Override
public String toString() {
	return "SensorData [key=" + key + ", value=" + value + ", timestamp=" + timestamp + "]";
}




}

