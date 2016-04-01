import receiver.Reader;
import sender.Writer;

public class Coordinator {
	
	public static void main (String[] args){
		//Writer w = new Writer();
		Reader r = new Reader();
		
		new Thread(r).start();
		//new Thread(w).start();
	}

}
