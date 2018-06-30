package com.practice.flink.model;

import java.util.Map;

public class ReceivedData {
private String mtype;
private Map<String,Object> data;
private Long timestamp;
public String getMtype() {
	return mtype;
}
public void setMtype(String mtype) {
	this.mtype = mtype;
}

public Map<String, Object> getData() {
	return data;
}
public void setData(Map<String, Object> data) {
	this.data = data;
}
public Long getTimestamp() {
	return timestamp;
}
public void setTimestamp(Long timestamp) {
	this.timestamp = timestamp;
}

}
