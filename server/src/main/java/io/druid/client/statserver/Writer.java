
package io.druid.client.statserver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/*
 * ref:
 * http://stackoverflow.com/questions/11463456/call-a-function-without-waiting-for-it
 */
public class Writer implements Runnable {

	
	public WriterLog log;
	
	public Writer() {
		this.log = new WriterLog();
	}

	public void writeEntry(String s){
		this.log.addSegID(s);
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			ServerSocket ss = new ServerSocket(5678);
			while(true){
				Socket s = ss.accept();
				System.out.println("connected!");
				ServerThread t = new ServerThread(s, this);
				new Thread(t).start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private class ServerThread implements Runnable{
		
		Socket s;
		Writer self;

		public ServerThread(Socket s, Writer writer) {
			// TODO Auto-generated constructor stub
			this.s = s;
			this.self = writer;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			try {
				//http://www.cs.odu.edu/~cs476/fall14/lectures/javasockets.htm
				BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
				PrintWriter sw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(s.getOutputStream())),true);
				if(br.readLine().equals("request")){
					//logger push the log entries
				    //System.out.println(br.readLine());
					//sw.println("segid1");
					//sw.println("segid2");
					
					//send the ip first
					sw.println(InetAddress.getLocalHost());
					sw.println(Long.toString(this.self.log.getCurrentTime()));
					//time has been overwritten
					ArrayList<String> ret = this.self.log.getLog();
					for(String s: ret){
						sw.println(s);
					}
					sw.println("end");
				}
				//else if(br.readLine().equals("stop")){
				//	this.s.close();
				//}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

  
}