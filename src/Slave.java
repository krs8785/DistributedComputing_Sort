
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Scanner;

public class Slave extends UnicastRemoteObject implements SlaveInterface {
	
	private static final String RESULT_FILENAME = "job1";
	public static NodeInfo node;
	private static final long serialVersionUID = 2L;
	private final static String master_IP = "10.10.10.103";
	public String slave_IP = null ;
	private String fileName = null;
	//sonam
	public HashSet<Integer> uniqueData = null;
	protected Slave() throws RemoteException {
		super();
		try {
			NetworkInterface eth1 = NetworkInterface.getByName("eth0");
			Enumeration<InetAddress> en = eth1.getInetAddresses(); 
			
			while(en.hasMoreElements()){
				InetAddress address = en.nextElement();
				if(!address.isLinkLocalAddress())
				{
					System.out.println("address::"+address.getHostAddress());
					slave_IP = address.getHostAddress();
				}
			}
			//slave_IP = InetAddress.getLocalHost().getHostAddress();
			System.out.println("slave ip::"+slave_IP);
			node = new NodeInfo();
			node.setNodeID(slave_IP);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}



	
	@Override
	public void readFile(NodeInfo n	) throws RemoteException {
		// TODO Auto-generated method stub
		try {
			//System.out.println("read file reach )))))))))))))))))");
			//System.setProperty("java.rmi.server.hostname", master_IP);
//			Registry reg = LocateRegistry.createRegistry(2828);
//			reg.rebind("Slave", this);
			Registry regi = LocateRegistry.getRegistry(master_IP, 9998);
			MasterInterface bsObj = (MasterInterface) regi.lookup("Master");
			//System.out.println("Slave Ready to host file");
			//System.out.println("readFile slave"+node.jobFileName+" "+node.resultFileName);
			File newFile = new File(node.getResultFileName());
			
			FileInputStream inStream = new FileInputStream(newFile);
			byte [] data = new byte [2048 * 2048];
			int len = inStream.read(data);
			 while(len > 0){
				 bsObj.sendFile(node, data, len);	 
				 len = inStream.read(data);				 
			 }
			 inStream.close();
			// System.out.println("about to delete---------------");
			// newFile.delete();
		 }catch(Exception e){
			 e.printStackTrace();
		 }
		
		//return true;
	}
	
	@Override
	public void sendFile(NodeInfo n, byte[] data, int len)
			throws RemoteException {
		try{
			//System.out.println("data ::"+data);
			//System.out.println("len ::"+len);
			if(!n.getJobFileName().equals(fileName)){
				if(null == fileName)
					fileName = n.getJobFileName();
				else
					fileName = String.format("%s_%s", fileName, n.getJobFileName());
			}
			
        	File file = new File(n.getJobFileName());
        	if(file.exists())
        		file.delete();
        	
        	file.createNewFile();
        	FileOutputStream outStream = new FileOutputStream(file,true);
        	outStream.write(data,0,len);
        	outStream.flush();
        	outStream.close();

        }catch(Exception e){
        	System.out.print("in catch in send file slaveeeee");
        	e.printStackTrace();
        }

		
	}
	
	@Override
	public void doneReadingFile(boolean merge) throws RemoteException {
		MasterInterface master = null;
		String[] filenames = null;
		
		try{
			System.out.println("filenameee : "+fileName);
			node.setJobFileName(fileName);
			 this.fileName = null;
			if(!merge){
				String name = node.getJobFileName();
				File file = new File(name);
	        	System.out.println("Done writing data...");
	        	slaveSortFile(file);
	        	System.out.println("before delete ::"+node.getJobFileName()+" ->"+name);
	        	if(file.delete()){
	        		System.out.println("yes del");//not able to delete
	        	}
	        	readFile( node);
			}
			else{
				System.out.println("merge "+node.jobFileName);
				
				if(node.getJobFileName().contains("_")){
					filenames = node.getJobFileName().split("_");
				}
				
				if (null != filenames) {
					File file1 = new File(filenames[0]);
					File file2 = new File(filenames[1]);
					mergeTwoFiles(file1, file2);					
					file1.delete();
					file2.delete();
					readFile(node);
				}
				
			}

        	
        }catch(Exception e){
        	e.printStackTrace();
        }

		
	}

	public static void main(String[] args) {
		try {
			
			
			Slave s = new Slave();
			System.out.println("in slave ip65 "+s.slave_IP);
			System.setProperty("java.rmi.server.hostname", s.slave_IP);
			System.out.println("slave obj created!!");
			Registry regclient = LocateRegistry.createRegistry(8889);
			System.out.println("slave obj bound!!");
			regclient.rebind("Slave", s);
			System.out.println("Slave is on..");

			Registry regi = LocateRegistry.getRegistry(master_IP, 9998);
			System.out.println("slave obj bound1111!!");
			MasterInterface bsObj = (MasterInterface) regi.lookup("Master");
			System.out.println("slave obj bound2222!!");
			bsObj.SlaveUP(s, node);
			System.out.println("went to slave  up");

		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("in catch for slave main");
			e.printStackTrace();
			//Runtime.getRuntime().exec();
		}

	}

	public void slaveSortFile(File f) throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("Got the file");
		
		BufferedReader br = null;
		ArrayList<String> input_from_file_temp = new ArrayList<String>();
		try { 
			String sCurrentLine; 
			br = new BufferedReader(new FileReader(f)); 
			while ((sCurrentLine = br.readLine()) != null) {
				//System.out.println(sCurrentLine);
				if (sCurrentLine.length() > 0)
					input_from_file_temp.add(sCurrentLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		
		//store in an array
		// ***** 
		// IMPORTANT NOTE -- what if million number ?? array will go out of memory ?? 
		// give second thought
		int[] input_from_file  = new int[input_from_file_temp.size()];
		for(int i=0;i<input_from_file_temp.size();i++){
			//System.out.println("size----"+input_from_file_temp.get(i).length());
			input_from_file[i] = Integer.parseInt(input_from_file_temp.get(i));
		}
		//print(input_from_file);
		//sonam
		this.setUniqueData(input_from_file);
		this.mergeSort(input_from_file);
		System.out.println("done..soritng...");
		//this.print(input_from_file);
		
		writeToFile(input_from_file, f.getName());
	}
	
public static  void mergeTwoFiles(File f1, File f2){
		
		System.out.println("hi+++");
		
		
		String resultFileName = String.format("%s%s.txt", RESULT_FILENAME,node.jobFileName.replaceAll("\\D+",""));
		File temp = new File(resultFileName);
		
		try (Writer w = new BufferedWriter(new OutputStreamWriter(
	              new FileOutputStream(temp)))) {
	

			BufferedReader br = new BufferedReader(new FileReader(f1));
			BufferedReader br1 = new BufferedReader(new FileReader(f2));
			Scanner sc1=new Scanner(br);
			Scanner sc2=new Scanner(br1);
			int d1 = Integer.parseInt(sc1.next());
			int d2 = Integer.parseInt(sc2.next());
			
			//System.out.println("mergetwo files ........"+d1+" "+d2);
			while(true){
			//	System.out.println("mergetwo files in while........"+d1+" "+d2);
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
			
			node.resultFileName = resultFileName;
		
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
	
	
	public void writeToFile (int []sortedList, String oldFileName) {
		String resultFileName = String.format("%s%s.txt", RESULT_FILENAME, 
				oldFileName.replaceAll("\\D+",""));
		
		
		try (BufferedWriter outputWriter = new BufferedWriter(new FileWriter(resultFileName));) {			
			for (int i = 0; i < sortedList.length; i++) {
				outputWriter.newLine();
				outputWriter.write(Integer.toString(sortedList[i]));
			}
			outputWriter.flush();  			
			//rmi call to server
//			Registry regi2 = LocateRegistry.getRegistry(master_IP, 9898);
//			MasterInterface bsObj2 = (MasterInterface) regi2.lookup("Master");
			//File f = new File(".",resultFileName);
			node.resultFileName = resultFileName;
//			bsObj2.receiveFileAfterSort(f,node);

			
			//f.delete();
        }catch (IOException e) {
            System.out.println("Error writing file");
            e.printStackTrace();
        } 
	}
	
	public void mergeSort(int item[]) {
		int n=item.length;
		if (n == 1)
			return;
		int mid = n / 2;
		int left[] = new int[mid];
		int right[] = new int[n - mid];
		for (int i = 0; i < mid; i++) {
			left[i] = item[i];
		}
		for (int i = mid; i < n; i++) {
			right[i - mid] = item[i];
		}
		mergeSort(left);
		mergeSort(right);
		this.mergeFj(left, right, item);
	}

	
	/**
	 * Merge fj.
	 *
	 * @param left the left
	 * @param right the right
	 * @param item the item
	 */
	public void mergeFj(int[] left, int[] right, int item[]) {
		// TODO Auto-generated method stub
		int lsize = left.length;
		int rsize = right.length;
		int lindex = 0;
		int rindex = 0;
		int uindex = 0;

		//Compares the values and puts back the in sorted  order.
		for (lindex = 0, rindex = 0, uindex = 0; lindex < lsize
				&& rindex < rsize;) {
			if (left[lindex] >= right[rindex])
				item[uindex++] = right[rindex++];
			else if (left[lindex] <= right[rindex])
				item[uindex++] = left[lindex++];
		}
		//for excess  values.
		while (lindex < lsize) {
			item[uindex++] = left[lindex++];
		}
		while (rindex < rsize) {
			item[uindex++] = right[rindex++];
		}
	}

	@Override
	public File sendFile(Object sendFile, String MethodType, NodeInfo node) throws RemoteException {
		System.out.println("got file from server");
		//check method name
		if (sendFile instanceof java.io.File) {
			File file = (File) sendFile;
			slaveSortFile(file);
		} else if (sendFile instanceof java.util.ArrayList<?>) {
			@SuppressWarnings("unchecked")
			ArrayList<File> list = (ArrayList<File>)sendFile;
			if (list.size() == 2) {
				mergeTwoFiles(list.get(0), list.get(1));
			}
		} else {
			System.out.println("ERorr : wrong type of class");
		}
		
		return null;
	}
	
	private void setUniqueData(int[] unsortedData){
		
		uniqueData = new HashSet<Integer>();
		
		for(int i=0;i<unsortedData.length;i++){
			uniqueData.add(unsortedData[i]);
		}
	}
	@Override
	public HashSet<Integer> getUniqueData() {
		return uniqueData;
	}
}
