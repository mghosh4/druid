package sender;

import java.util.ArrayList;

/*
 * ref: 
 * http://stackoverflow.com/questions/732034/getting-unixtime-in-java
 */
public class WriterLog {
	
	Long currentTime;
	Long previousTime;
	ArrayList<String> curSegIDList;
	ArrayList<String> prevSegIDList;
	
	public WriterLog() {
		currentTime = System.currentTimeMillis() / 1000L;
		previousTime = System.currentTimeMillis() / 1000L;
		curSegIDList = new ArrayList<String>();
		prevSegIDList = new ArrayList<String>();
	}
	
	public synchronized void addSegID(String segID){
		curSegIDList.add(segID);
	}
	
	public synchronized ArrayList<String> writeLog(){
		previousTime = currentTime;
		currentTime = System.currentTimeMillis() / 1000L;
		prevSegIDList = curSegIDList;
		curSegIDList = new ArrayList<String>();
		return prevSegIDList;
	}

}
