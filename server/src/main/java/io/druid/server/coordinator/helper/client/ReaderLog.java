package main.java.io.druid.server.coordinator.helper.client;

import java.util.HashMap;

public class ReaderLog {
	
	HashMap<String, Long> versionMap;
	HashMap<String, Integer> countMap;
	
	public ReaderLog() {
		versionMap = new HashMap<String, Long>();
		countMap = new HashMap<String, Integer>();
	}
	
	public synchronized void updateVersion(String ip, Long time){
		this.versionMap.put(ip, time);
	}
	
	public Long getVersion(String ip){
		if(this.versionMap.containsKey(ip)){
			return this.versionMap.get(ip);
		}
		else
			return (long) -1;
	}
	
	public synchronized void addSegID(String seg){
		if(this.countMap.containsKey(seg)){
			this.countMap.put(seg, this.countMap.get(seg)+1);
		}
		else{
			this.countMap.put(seg, 1);
		}
	}
	
	public void printCountMap(){
		for(String seg: this.countMap.keySet()){
			System.out.println("seg ID: "+seg+" Count:"+this.countMap.get(seg));
		}
	}
	
	public void printVersionMap(){
		for(String ip: this.versionMap.keySet()){
			System.out.println("IP: "+ip+" Latest Read Version:"+Long.toString(this.versionMap.get(ip)));
		}
	}
	
	
}
