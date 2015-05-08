import java.io.File;
import java.io.FileInputStream;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;


public class HelperThread extends Thread {

	private NodeInfo slaveNode;
	
	public HelperThread (NodeInfo node) {
		this.slaveNode = node;
	}
	
	public void run() {
		try {
			readFile(slaveNode);
		} catch (RemoteException e) {
			System.out.println("Erroooooooorrrrrr in RMI///////////////");
			e.printStackTrace();
		}
	}
	
	public String [] retrieveJobFileNames (String fileName) {
		if (fileName != null) {
			String [] str = fileName.split("\\_");
			return str;
		}
		
		return null;
	}
	
	public void readFile(NodeInfo node) throws RemoteException {
		System.out.println("Sending job to ip.............."+slaveNode.nodeID);
		if (node.jobFileName != null) {
			String [] nameList = this.retrieveJobFileNames(node.jobFileName);
			int count = nameList.length;
			try {
				
				//System.setProperty("java.rmi.server.hostname", node.nodeID);
				//Registry reg = LocateRegistry.createRegistry(9898);
				//reg.rebind("Master", this);
				Registry reg = LocateRegistry.getRegistry(2829);
				SlaveInterface bsObj = (SlaveInterface) reg.lookup("Slave");
				//bsObj.s
				//System.out.println("Mater ready to send job...");
				
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
						 bsObj.sendFile(node, data, len);	 
						 len = inStream.read(data);
						 if (len < 0 && i == count) {
							 bsObj.doneReadingFile((count == 2));
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
}
