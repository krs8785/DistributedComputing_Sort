


import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;

public class MonitorClass extends Thread{

	public volatile int isAnyThreadRunning = 1;
	
	ArrayList<String> ipList;
	Master obj;
	private static boolean stop = true;
	
	public MonitorClass(ArrayList<String> ipLists, Master master) {
		this.ipList = ipLists;
		System.out.println("entering constrcutor of monitor");
		this.obj = master;
	}

	
	public void run(){
		System.out.println("==================== start monitor====================");
		for(String ip:ipList){
			PingClass pg = new PingClass(ip);
			Thread t1 = new Thread(pg);
			//t1.setDaemon(true);
			t1.start();


		}
	}
	
	private void updateStatus(String ip, boolean status){
		if(status){
			System.out.println("ip========"+ip+"............"+"Aliveeeeee");
			obj.ipAlive(ip,"ALIVE");
		}else{
			System.out.println("ip========"+ip+"............"+"deadddddddd");
			obj.ipDead(ip,"NOTALIVE");
		}
	}
	
	public static void kill() {
		stop = false;
	}
	
	private class PingClass implements Runnable{
		
		
		private String ipAddr;
		
		public PingClass(String ip) {
			this.ipAddr = ip;
		}
		
		public void run() {
			//String ipAddress[] = {"192.168.1.12","192.168.1.109","192.168.1.122","192.168.1.121","192.168.1.10"};
			//String ipAddress[] = {"129.21.60.8","129.21.115.53","129.21.60.75","129.21.60.65"};
			try {
				do{
				//System.out.println("How would it be Evil!?");
				//for(int i=0;i<4;i++){
					//System.out.println("1");
					//System.out.println("IP IS::"+ipAddr);
					InetAddress inet = InetAddress.getByName(ipAddr);
					//System.out.println("2");
					boolean reachable = inet.isReachable(3000);
					//System.out.println("it is: "+reachable);
					
				//}
				//System.out.println("Now sleeping!!");
//				Thread.sleep(5000);
//				if(reachable)
//					Master.firstNodecheckCompleted = true;
					System.out.println("checking ================"+ipAddr);
				updateStatus(ipAddr,reachable);
				//Thread.sleep(5000);
				}while(stop);
			} catch (IOException e) {
				System.out.println("dgsdgs===="+e.getStackTrace());
				isAnyThreadRunning = 0;
			}
			catch(Exception e){
				System.out.println("sdada====");
				e.printStackTrace();
				isAnyThreadRunning = 0;
			}
		}
		
	}
	
//	public static void main(String []arg) {
//	MonitorClass th=new MonitorClass();
//	Thread t1 = new Thread(th);
//	t1.setDaemon(true);
//	t1.start();
//	//System.out.println(t1.isDaemon());
//	//		    try {
//	//		    Thread.sleep(1000);
//	//		    } catch (Exception e) {
//	//	        }
//	//		    if (t1.isAlive()){
//	//		    	System.out.println("is alive!!");
//	//		    }
//	//		    else
//	//		    	System.out.println("is dead!!");
//
//	do{
//	}while(th.isAnyThreadRunning >0);
//
//
//}
	
}
