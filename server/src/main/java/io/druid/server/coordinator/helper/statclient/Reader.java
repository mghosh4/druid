package io.druid.server.coordinator.helper.statclient;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import io.druid.server.coordinator.helper.statclient.ReaderLog;

public class Reader implements Runnable{
	

	ReaderLog log;
	@Override
	public void run() {
		// TODO Auto-generated method stub
		log = new ReaderLog();
		InetAddress addr;
		try {
			addr = InetAddress.getByName("127.0.0.1");
			System.out.println("addr = " + addr);
		    
		    while(true){
		    	Socket socket =  new Socket(addr, 5678);
			    ClientThread ct = new ClientThread(socket, this);
		    	ct.run();
		    	Thread.sleep(5000);
		    }
		    
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	     
	}

}

class ClientThread implements Runnable{

	Socket s;
	Reader self;
	
	public ClientThread(Socket s, Reader reader){
		this.s = s;
		this.self = reader;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			//http://www.cs.odu.edu/~cs476/fall14/lectures/javasockets.htm
			BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			PrintWriter sw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(s.getOutputStream())),true);
			//System.out.println("1");
			/*if(br.readLine().equals("request")){
				//logger push the log entries
			}
			else if(br.readLine().equals("stop")){
				this.s.close();
			}*/
			sw.println("request");
			String st = br.readLine();
			//should be ip
			String ip = st;
			st = br.readLine();
			Long current = Long.parseLong(st);
			this.self.log.updateVersion(ip, current);
			//while(!st.equals("end")){
			while(!st.equals("end")&&this.self.log.getVersion(ip)<=current){
				//store string: segment id
				System.out.println(st);
				st = br.readLine();	
				this.self.log.addSegID(st);
			}
			this.self.log.printVersionMap();
			this.self.log.printCountMap();
			//sw.println("stop");
			//this.s.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}