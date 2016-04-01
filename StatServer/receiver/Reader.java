package receiver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class Reader implements Runnable{
	

	@Override
	public void run() {
		// TODO Auto-generated method stub
		InetAddress addr;
		try {
			addr = InetAddress.getByName("127.0.0.1");
			System.out.println("addr = " + addr);
		    Socket socket =  new Socket(addr, 5678);
		    ClientThread ct = new ClientThread(socket);
		    while(true){
		    	ct.run();
		    	wait(5000);
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
	
	public ClientThread(Socket s){
		this.s = s;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			//http://www.cs.odu.edu/~cs476/fall14/lectures/javasockets.htm
			BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			PrintWriter sw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(s.getOutputStream())),true);
			/*if(br.readLine().equals("request")){
				//logger push the log entries
			}
			else if(br.readLine().equals("stop")){
				this.s.close();
			}*/
			sw.println("request");
			String st = br.readLine();
			while(!st.equals("end")){
				//store string: segment id
				st = br.readLine();
			}
			sw.println("stop");
			this.s.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}