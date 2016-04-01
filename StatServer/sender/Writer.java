
package sender;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/*
 * ref:
 * http://stackoverflow.com/questions/11463456/call-a-function-without-waiting-for-it
 */
public class Writer implements Runnable {

	
	WriterLog log;
	
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
				ServerThread t = new ServerThread(s);
				new Thread(t).start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private class ServerThread implements Runnable{
		
		Socket s;

		public ServerThread(Socket s) {
			// TODO Auto-generated constructor stub
			this.s = s;
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
					
				}
				else if(br.readLine().equals("stop")){
					this.s.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

  
}