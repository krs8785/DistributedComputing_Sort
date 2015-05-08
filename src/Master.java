

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;


public class Master extends UnicastRemoteObject implements MasterInterface {
	
	private JobManager jManager;
	private static final String RESULT_FILENAME = "mas1";
	private static boolean masterStarted;
	private NodeInfo masterNode;
	private String master_IP;
	//sonam
	public HashSet<Integer> uniqueDataAtMaster = new HashSet<Integer>();
	
	public static boolean firstNodecheckCompleted = false;

	private static final long serialVersionUID = 1L;
	
	//static ArrayList<String> pi_IP = new ArrayList<String>();
	public HashMap<String,String> amIAlive = new HashMap<String,String>();
	private ArrayList<String> ipList = new ArrayList<String>();
	private ArrayList<String> inactiveList = new ArrayList<String>();
	private ArrayList<NodeInfo> activeList = new ArrayList<NodeInfo>();
	private ArrayList<String> sendFileList = new ArrayList<String>();
	
	private boolean startJobs;
	
	public void updateStartJob(boolean start) {
		this.startJobs = start;
	}
	
	public synchronized void updateAmIAliveList(String ip, String status){
		amIAlive.put(ip, status);
		
		if("NOTALIVE".equals(status)){
			NodeInfo toRemove = null;
			for(NodeInfo value:this.activeList){
				//System.out.println("value in Active--"+value.toString());
				if(value.nodeID.equals(ip))
					toRemove = value;
			}
			activeList.remove(toRemove);
			// TODO :need to pass nodeinfo to job manager
			jManager.nodeFailureWithError(toRemove, "Node Failed"+toRemove.nodeID);
			inactiveList.add(ip);
		}
		else if("ALIVE".equals(status)){
			inactiveList.remove(ip);
		}
			
	}
	
	protected Master() throws RemoteException {
		super();
		masterStarted = false;
		this.startJobs = false;
		try {
			NetworkInterface eth1 = NetworkInterface.getByName("eth0");
			Enumeration<InetAddress> en = eth1.getInetAddresses(); 
			
			while(en.hasMoreElements()){
				InetAddress address = en.nextElement();
				if(!address.isLinkLocalAddress())
				{
					System.out.println("address::"+address.getHostAddress());
					master_IP = address.getHostAddress();
				}
			}
			
			System.out.println("IP is::: "+InetAddress.getLocalHost().getHostAddress());
			System.out.println("master ip ::"+master_IP);
			jManager = JobManager.getInstance();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	
	//call when result true
	public void ipAlive(String ip, String status){
		//System.out.println("access InactiveList for--"+ip);
		/*
		for(String value:this.inactiveList){
			System.out.println("value in InActive Before--"+value);
		}
		updateInactiveList(ip);
		*/
		for(String val:sendFileList){
			if(val.equals(ip)){
				System.out.println("Sending filesss===============--"+ip);
				this.wakeUpSlave(ip);
				break;
			}
		}
		if(null != sendFileList || !sendFileList.isEmpty())
			updateSendFileList(ip);
		
		for(String value:this.inactiveList){
			System.out.println("value in InActive Before--"+value);
		}
		
		for(String val:inactiveList){
			if(val.equals(ip)){
				System.out.println("Sending filesss===============--"+ip);
				this.runSlave(ip);
				break;
			}
		}
		updateAmIAliveList( ip,  status);
		
//		for(String value:this.inactiveList){
//			System.out.println("value in InActive After--"+value);
//		}
		//System.out.println("=================================================================");
	}
	
	private synchronized void updateSendFileList(String ip){
				sendFileList.remove(ip);
	}
	
	public void ipDead(String ip, String status){
	//	System.out.println("access ActiveList for--"+ip);
		updateAmIAliveList( ip,  status);
		
	}
	
	public void wakeUpSlave(String ip){
		
		//TODO : code to wake up slave and execute slaveup.java
		try{
			
			JSch j = new JSch();
			Session session = j.getSession("pi",ip,22); //trying on master itself testing but will be other pi's information
			session.setPassword("raspberry");
			Properties config = new Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);
			session.connect();
			//boolean ptimestamp  = true;
			String command = "mkdir -p SlaveFiles"; //this is created in the pi folder so i guees this thing is working
			
			Channel channel = session.openChannel("exec");
			ChannelExec ce = (ChannelExec) channel;
			ce.setCommand(command);
			ce.setErrStream(System.err);
			ce.connect();
			BufferedReader br = new BufferedReader(new InputStreamReader(ce.getInputStream()));
			String line;
			while((line = br.readLine()) != null){
				System.out.println(line);
			}
			ce.disconnect();
			session.disconnect();
				
			//System.out.println("send file call for ip::"+ip);
			this.sendFiles(ip);
				
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		//System.out.println("done");
		
	}
	//call when result true
//	public void accessInactiveList(String ip, boolean status){
//		//System.out.println("access InactiveList for--"+ip);
//		
//		
//		/*for(String value:this.inactiveList){
//			System.out.println("value in InActive Before--"+value);
//		}*/
//		updateInactiveList(ip);
//		
//		for(String val:sendFileList){
//			if(val.equals(ip)){
//				System.out.println("waking up the  slvae--"+ip);
//				//this.wakeUpSlave(ip);
//				break;
//			}
//		}
//		
//		//this.runSlave(ip);
//		/*for(String value:this.inactiveList){
//			System.out.println("value in InActive After--"+value);
//		}*/
//		System.out.println("=================================================================");
//	}
	
	
//	public void accessActiveList(String ip, boolean status){
//	//	System.out.println("access ActiveList for--"+ip);
//		
//		NodeInfo toRemove = new NodeInfo();
//		for(NodeInfo value:this.activeList){
//			//System.out.println("value in Active--"+value.toString());
//			if(value.nodeID.equals(ip))
//				toRemove = value;
//		}
//		
//		// TODO :need to pass nodeinfo to job manager
//		jManager.nodeFailureWithError(toRemove, "Node Failed"+toRemove.nodeID);
//		
//	}
	
	
	
	private void runSlave(String ip){
		try{

			JSch j = new JSch();
			Session session = j.getSession("pi",ip,22); //trying on master itself testing but will be other pi's information
			session.setPassword("raspberry");
			Properties config = new Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);
			session.connect();
			//boolean ptimestamp  = true;
			String command = "cd ~/SlaveFiles && chmod 755 RunSlaveScript.sh && ./RunSlaveScript.sh"; //this is created in the pi folder so i guees this thing is working

			Channel channel = session.openChannel("exec");
			ChannelExec ce = (ChannelExec) channel;
			ce.setCommand(command);
			ce.setErrStream(System.err);
			ce.connect();
			BufferedReader br = new BufferedReader(new InputStreamReader(ce.getInputStream()));
			String line;
			while((line = br.readLine()) != null){
				System.out.println(line);
			}
			ce.disconnect();
			session.disconnect();
			//System.out.println("slave files compiled and run"+ip);


		}catch(Exception e){
			e.printStackTrace();
		}

		//System.out.println("done");
	}
	
	private void sendFiles(String ip){
		JSch jsch = new JSch();
		Session session = null;

		try {
			session = jsch.getSession("pi", ip, 22);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setPassword("raspberry");
			session.connect();
			ChannelSftp sftpChannel = (ChannelSftp)session.openChannel("sftp");
			sftpChannel.connect();
			//ChannelSftp sftpChannel = (ChannelSftp) channel;
			File file = new File(".","Slave.class");
			sftpChannel.cd("SlaveFiles/");
			sftpChannel.put(new FileInputStream(file),"Slave.class");
			file = new File(".","SlaveInterface.class");
			sftpChannel.put(new FileInputStream(file),"SlaveInterface.class");
			file = new File(".","NodeInfo.class");
			sftpChannel.put(new FileInputStream(file),"NodeInfo.class");
			file = new File(".","MasterInterface.class");
			sftpChannel.put(new FileInputStream(file),"MasterInterface.class");
			file = new File(".","RunSlaveScript.sh");
			sftpChannel.put(new FileInputStream(file),"RunSlaveScript.sh");
			
			String command = "chmod 755 RunSlaveScript.sh"; //this is created in the pi folder so i guees this thing is working
			Channel channel = session.openChannel("exec");
			ChannelExec ce = (ChannelExec) channel;
			ce.setCommand(command);
			ce.setErrStream(System.err);
			ce.connect();
			BufferedReader br = new BufferedReader(new InputStreamReader(ce.getInputStream()));
			String line;
			while((line = br.readLine()) != null){
				System.out.println(line);
			}
			ce.disconnect();
			
			sftpChannel.exit();
			session.disconnect();
			sendFileList.remove(ip);
		} catch (JSchException e) {
			e.printStackTrace(); 
		} catch (SftpException e) {
			e.printStackTrace();
		} catch(FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) throws IOException {
		
		System.out.println("starting master");

		Master master = new Master();
		//String master_Ip = null;
		try {
			/*NetworkInterface eth1 = NetworkInterface.getByName("eth1");
			Enumeration<InetAddress> en = eth1.getInetAddresses(); 
			
			while(en.hasMoreElements()){
				InetAddress address = en.nextElement();
				if(!address.isLinkLocalAddress())
				{
					System.out.println("address::"+address.getHostAddress());
					master_Ip = address.getHostAddress();
				}
			}*/

			//System.setProperty("java.rmi.server.hostname", "192.168.1.7");
			Registry reg = LocateRegistry.createRegistry(9998);
			reg.rebind("Master", master);
			System.out.println("Master is on...");
			//master.jManager.splitFile(new File("/home/stu1/s790/scp8613/RSPI_DistributedSort/RSPI_NEW/Data/sample_dataset.txt"));
			master.jManager.splitFile(new File("small_sample.txt"));
		
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}

		try (BufferedReader br = new BufferedReader(new FileReader("IpList.txt"))) {
			String line;
			while ((line = br.readLine()) != null) {
				//System.out.println(line);
				master.amIAlive.put(line, "NOTALIVE");
				master.ipList.add(line);
				master.sendFileList.add(line);
				master.inactiveList.add(line);
			}
		}

		//master.monitorNodes(master.inactiveList);
		MonitorClass monitor=new MonitorClass(master.ipList,master);
		Thread t1 = new Thread(monitor);
		//t1.setDaemon(true);
		t1.start();
		
		System.out.println("wait");
		while (!master.startJobs) {
			
		}
		System.out.println("start job---------------------------------------------------");
		System.out.println("after job-----???"+master.jManager.isJobListEmpty());
		System.out.println("after111 job size-----???"+master.jManager.resultJobListCount());
		
		
		while (!master.jManager.isJobListEmpty() && 
				 master.jManager.resultJobListCount()  != 4) {
			System.out.println("jobbb send");
			for (NodeInfo node : master.activeList) {
				if (!node.isBusy) {
					System.out.println("job111111 send"+node.nodeID);
					master.jManager.getNextFileForNode(node, null);
					node.setBusy(true);
					master.updateNodeAtActiveList(node);
					new HelperThread(node).start();	
				}
			}
		}
		
		while (master.jManager.hasInterimJobList() || !master.jManager.isJobListEmpty()) {
			for (NodeInfo node : master.activeList) {
				if (!node.isBusy) {
					System.out.println("jobbbbbbbb send"+node.nodeID);
					master.jManager.getNextFileForNode(node, null);
					node.setBusy(true);
					master.updateNodeAtActiveList(node);
					new HelperThread(node).start();	
				}
			}
		}

	}

	public synchronized void updateNodeAtActiveList (NodeInfo node) {
		System.out.println("***********updateNodeAtActiveList method*******");
		NodeInfo tempNode = null;
		for (NodeInfo n : this.activeList) {
			if (n.nodeID.equals(node.nodeID)) {
				tempNode = n;
				break;
			}
		}
		if (tempNode != null)
			this.activeList.remove(tempNode);
		
		this.activeList.add(node);
	}
	
	@Override
	public void SlaveUP(SlaveInterface slave, NodeInfo node) throws RemoteException {
		System.out.println("**************************************************Slave :"+node.nodeID+" joined here.");	
		//readFile(slave, node);
		this.activeList.add(node);
		if(null != node){
			System.out.println(node.nodeID + ":::is alive");
			amIAlive.put(node.nodeID, "ALIVE");
		}
		this.updateStartJob(true);
		
//			Object sendFile = jManager.getNextFileForNode(node, null);
//			System.out.println("call to send file!!!");
//			bsObj.sendFile(sendFile, "", node);
	}

	@Override
	public void receiveFileAfterSort(File file, NodeInfo node) throws RemoteException {
//		// TODO Auto-generated method stub
//		NodeInfo tempNode = node;
//		try {
//			Object sendFile = null;
//			jManager.getNextFileForNode(tempNode, file);
//			System.setProperty("java.rmi.server.hostname", "192.168.1.13");
//			Registry regi = LocateRegistry.getRegistry(node.nodeID, 2828);
//			SlaveInterface bsObj = (SlaveInterface) regi.lookup("Slave");
//			bsObj.sendFile(sendFile, "", tempNode);
//			
//			updateNodeAtMasterList(tempNode);
//		} catch (NotBoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		//send to job manager
	}
	
	public void startSortAtMaster(NodeInfo mNode) {
		jManager.getNextJobFilesForMaster(mNode);
		if(mNode.jobFileName != null){
			this.mergeTwoFiles(mNode);
		}
		else{
			System.out.println("Finally calculaing the average!!!!!!!!!!!");
			calculateAverage();
		}
	}
	
	@Override
	public void readFile(NodeInfo node) throws RemoteException {
		//System.out.println("entering read filke 1");
		//System.out.println("entering read 2"+node.jobFileName);
		SlaveInterface slave = null;
		masterStarted = jManager.getNextFileForNode(node, null);
		if (node.jobFileName == null && masterStarted) {
			System.out.println("Enter master @#@$@#$@#$@#$@#$@#$@#$@#$@#");
			MonitorClass.kill();
			this.masterNode = new NodeInfo();
			masterNode.jobFileName = null;
			masterNode.resultFileName = null;
			this.startSortAtMaster(masterNode);
		} else if (node.jobFileName != null) {
			System.out.println("enter non master #@%$#@$%^$^$#@%^^#@$%^");
			String [] nameList = jManager.retrieveJobFileNames(node.jobFileName);
			int count = nameList.length;
			
			try {
				//System.err.println("inside READ FILE %%%%%%%%");
				System.setProperty("java.rmi.server.hostname", node.nodeID);
				//Registry reg = LocateRegistry.createRegistry(9898);
				//reg.rebind("Master", this);
				//Registry reg = LocateRegistry.getRegistry(2829);
				//SlaveInterface bsObj = (SlaveInterface) reg.lookup("Slave");
				//bsObj.s
				//System.out.println("Mater ready to send job...");
				updateNodeAtActiveList(node);
				for (int i = 1; i <= count; i++) {
					//System.out.println("_--------senging---"+i);
					node.jobFileName = nameList[i-1];
					File newFile = new File(node.jobFileName);
					FileInputStream inStream = new FileInputStream(newFile);
					byte [] data = new byte [2048 * 2048];
					int len = inStream.read(data);
					int a =0;
					 while(len > 0){
						// System.out.println("----------while-----"+a);
						 slave.sendFile(node, data, len);	 
						 len = inStream.read(data);
						 if (len < 0 && i == count) {
							 //slave.doneReadingFile(this, (count == 2));
						 }
					 }
					 inStream.close();
				}
				
				System.out.println("Comeplte readFile method...");
				 
			 }catch(Exception e){
				 e.printStackTrace();
			 }
		}
	}

	@Override
	public void sendFile(NodeInfo node, byte[] data, int len)
			throws RemoteException {
		// TODO Auto-generated method stub
		try{
        	File file = new File(node.resultFileName);
        	
        	if(file.exists())
        		file.delete();
        	
        	file.createNewFile();
        	FileOutputStream outStream = new FileOutputStream(file,true);
        	outStream.write(data,0,len);
        	outStream.flush();
        	outStream.close();
        	System.out.println("Done writing result...");
        	
        	System.out.println("dele job name"+node.jobFileName);
        	File jobFile = new File(node.jobFileName);
        	jobFile.delete();
        	node.setBusy(false);
        	updateNodeAtActiveList(node);
        	//sonam
//        	HashSet<Integer> uniqueData = slave.getUniqueData();
//        	if(null != uniqueData && node.resultFileName.contains("job100")){
////            	File file1 = new File(node.resultFileName+"_uniqueData");
////            	file1.createNewFile();
////            	FileOutputStream outStream1 = new FileOutputStream(file,true);
//            	Iterator<Integer> itr = uniqueData.iterator(); 
//            	while(itr.hasNext()){
//            		//System.out.println(itr.next()); 
//            		//outStream1.write(itr.next());
//            		uniqueDataAtMaster.add(itr.next());
//            	} 
////            	outStream1.flush();
////            	outStream1.close();
//        	}
        	
        	//readFile(node);
        }catch(Exception e){
        	e.printStackTrace();
        }
	}
	
	public void mergeTwoFiles(NodeInfo mNode){
		System.out.println("Master merge two files");
		String [] nameList = jManager.retrieveJobFileNames(mNode.jobFileName);
		if (nameList.length >= 2) {
			String resultFileName = String.format("%s%s.txt", RESULT_FILENAME,mNode.jobFileName.replaceAll("\\D+",""));
			File f1 = new File(nameList[0]);
			File f2 = new File(nameList[1]);
			System.out.println("taking in considertion---------------------"+nameList[0]+ " AND "+nameList[1] +" with "+resultFileName);
			File temp = new File(resultFileName);
			
			try (Writer w = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(temp)))) {


				BufferedReader br = new BufferedReader(new FileReader(f1));
				BufferedReader br1 = new BufferedReader(new FileReader(f2));
				Scanner sc1=new Scanner(br);
				Scanner sc2=new Scanner(br1);
				int d1 = Integer.parseInt(sc1.next());
				int d2 = Integer.parseInt(sc2.next());

				System.out.println("mergetwo files going  on........"+d1+" "+d2);
				while(true){
						//System.out.println("mergetwo files in while........"+d1+" "+d2);
					if(d1>=d2){
						w.write(String.valueOf(d2)+"\n");
						if(sc2.hasNext()){
							d2 = Integer.parseInt(sc2.next());
						}else{
							while(sc1.hasNext()){
								w.write(String.valueOf(d1)+"\n");
								d1 = Integer.parseInt(sc1.next());
							}
							w.write(String.valueOf(d1));
							break;
						}
					}else{
						w.write(String.valueOf(d1)+"\n");
						if(sc1.hasNext()){
							d1 = Integer.parseInt(sc1.next());
						}else{
							while(sc2.hasNext()){
								w.write(String.valueOf(d2)+"\n");
								d2 = Integer.parseInt(sc2.next());
							}
							w.write(String.valueOf(d2));
							break;
						}
					}				
				}
				sc1.close();
				sc2.close();
				br.close();
				br1.close();
				System.out.println("done MASTER MERGE");
				mNode.resultFileName = resultFileName;
				f1.delete();
				f2.delete();
				System.out.println("[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[ NEW ]]]]]]]]]]]]]]]]]]]]]]]]]]");
				//System.exit(1);
				this.startSortAtMaster(mNode);
			} catch (UnsupportedEncodingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
	
	
	//sonam	
		private void calculateAverage(){
			double average = 0.0;
			Iterator<Integer> itr = uniqueDataAtMaster.iterator(); 
	    	while(itr.hasNext()){
	    		//System.out.println(itr.next()); 
	    		//uniqueDataAtMaster.add(itr.next());
	    		average = average + itr.next();
	    	} 
	    	average =  average/uniqueDataAtMaster.size();
	    	System.out.println("Finalaverage is::"+average);
		}
		
		
}
