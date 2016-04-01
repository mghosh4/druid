import sender.Writer;

public class BrokerLogger {
	
	public static void main (String[] args) throws InterruptedException{
		Writer w = new Writer();
		//Reader r = new Reader();
		
		//new Thread(r).start();
		new Thread(w).start();
		//System.out.print("2");
		for(int i =0;i<5;i++){
			w.writeEntry("seg 1");
		}
		Thread.sleep(1000);
		for(int i =0;i<5;i++){
			w.writeEntry("seg 2");
		}
		Thread.sleep(2000);
		for(int i =0;i<5;i++){
			w.writeEntry("seg 3");
		}
		Thread.sleep(3000);
		for(int i =0;i<5;i++){
			w.writeEntry("seg 4");
		}
		Thread.sleep(4000);
		for(int i =0;i<5;i++){
			w.writeEntry("seg 5");
		}
	}

}
