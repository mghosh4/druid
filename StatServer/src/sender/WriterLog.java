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
	
	public synchronized ArrayList<String> getLog(){
		previousTime = currentTime;
		currentTime = System.currentTimeMillis() / 1000L;
		prevSegIDList = curSegIDList;
		curSegIDList = new ArrayList<String>();
		return prevSegIDList;
	}

	public Long getCurrentTime() {
		return currentTime;
	}

	public void setCurrentTime(Long currentTime) {
		this.currentTime = currentTime;
	}

	public Long getPreviousTime() {
		return previousTime;
	}

	public void setPreviousTime(Long previousTime) {
		this.previousTime = previousTime;
	}

}
