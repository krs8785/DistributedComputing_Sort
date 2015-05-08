
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;

public class JobManager {
	
	private final static int BLOCK_SIZE = 200;
	private final static String JOB_FILENAME = "job";
	private static final String RESULT_FILENAME = "job1";
	private final static Integer MAX_LEVEL = 4;
	private final static Integer LINES_READ = 100;
	private final static Integer CHARACTERS_PER_LINE = 2;
	
	private static JobManager manager;
	private static boolean masterSortLevel;
	private static boolean startMaster;
	private static Integer jobLevel;
	
	private JobTracker tracker;
	private String parentString;
	
	//private boolean startedMasterSort;
	
	private JobManager () {
		this.tracker = new JobTracker();
		jobLevel = 0;
		//this.startedMasterSort = false;
		masterSortLevel = false;
		startMaster = false;
	}
	
	public static JobManager getInstance () {
		if (manager == null)
			manager = new JobManager();
		
		return manager;
	}
	
	public boolean isJobListEmpty () {
		//System.out.println("joblist empty????????????????");
		System.out.println("after111 job size-----???"+this.tracker.jobList.size());

		return this.tracker.jobList.isEmpty();
	}
	
	public boolean hasInterimJobList () {
		//System.out.println("interimlist empty????????????????");
		return (this.tracker.interimJobList.size() > 0);
	}
	
	public int resultJobListCount () {
		return (this.tracker.resultList.size());
	}
	
	public class JobTracker {
		
		public ConcurrentLinkedQueue<String>jobList;
		
		private ArrayList<NodeInfo> nodeList;
		private ConcurrentLinkedQueue<String>resultList;
		private ArrayList<String>interimJobList;
		
		private ConcurrentLinkedQueue<String>masterJobList;
		private ConcurrentLinkedQueue<String>masterResultList;
		private Integer interimFileCount;
		
		public JobTracker () {
			this.nodeList = new ArrayList<NodeInfo>(8);
			this.jobList = new ConcurrentLinkedQueue<String>();
			this.resultList = new ConcurrentLinkedQueue<String>();
			this.interimJobList = new ArrayList<String>(8);
			
			this.masterJobList = new ConcurrentLinkedQueue<String>();
			this.masterResultList = new ConcurrentLinkedQueue<String>();
			this.interimFileCount = 0;
		}
		
		public void setNodeList(NodeInfo node) {
			if (node != null) {
				this.nodeList.add(node);
			}
		}
		
		public NodeInfo getNodeObject(String id) {
			if (this.nodeList.size() > 0) {
				Iterator<NodeInfo> itr = this.nodeList.iterator();
				NodeInfo node;
				while (itr.hasNext()) {
					node = itr.next();
					if (node.nodeID == id) {
						return node;
					}
				}
			}
			
			return null;
		}
		
		private String getFileNameWithoutExtension(String fileName) {
		    if (fileName != null) {
		    	int pos = fileName.lastIndexOf(".");
			    if (pos > 0) {
			        return fileName.substring(0, pos);
			    }
		    }
		    
		    return null;
		}
		
		public void resetTheJobForNextLevel() {
			if (this.resultList.peek() != null) {
				if (!masterSortLevel) {
					System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~LEVEL UP ~~~~~~~~~~~~~~~");
					jobLevel++;
					for (String jobFile : this.resultList) {
						this.jobList.add(jobFile);
					}
					this.resultList.clear();
				}
				
				if (jobLevel == 1 && this.interimJobList.size() > 0) {
					this.interimFileCount = this.interimJobList.size();
				} else if (jobLevel == MAX_LEVEL) {
					masterSortLevel = true;
					if (this.interimJobList.isEmpty()) {
						System.out.println("00000000000000000000000000--start master sort --0000000000000000000000000000000000000");
						startMaster = true;
						for (String jobFile : this.jobList) {
							this.masterJobList.add(jobFile);
						}
						for (String jobFile : this.masterResultList) {
							this.masterJobList.add(jobFile);
						}
						this.masterResultList.clear();
						this.jobList.clear();
					}
				} else {
					this.interimFileCount = 0;
				}
			}
		}
		
		public void resetJobForMaster () {
			System.out.println("reset for master");
			if (this.masterResultList.size() == 1) {
				System.err.println("sorted entire file");
				
				return;
			}
			for (String jobFile : this.masterResultList) {
				this.masterJobList.add(jobFile);
			}
			this.masterResultList.clear();
			jobLevel++;
			System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~LEVEL UP ~~~~~~~~~~~~~~~");
		}
		
//		public boolean restForNextLevel(boolean forMaster) {
//			if (this.resultList.peek() != null) {
//				if (!forMaster) {
//					if (this.resultList.size() == 1 && this.interimJobList.isEmpty()) {
//						System.err.println("sorted entire file");
//						System.exit(1);
//					}
//					
//					for (String jobFile : this.resultList) {
//						this.jobList.add(jobFile);
//					}
//					//this.jobList = this.resultList;
//					this.resultList.clear();
//					jobLevel++;
//					System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~LEVEL UP ~~~~~~~~~~~~~~~");
//					if (jobLevel == 1 && this.interimJobList.size() > 0) {
//						this.interimFileCount = this.interimJobList.size();
//					} else if (jobLevel == MAX_LEVEL) {
//						if (this.interimJobList.isEmpty()) {
//							
//						}
//						System.out.println("00000000000000000000000000--start master sort --0000000000000000000000000000000000000");
//						
//						this.masterJobList = new ConcurrentLinkedQueue<String>();
//						for (String jobFile : this.jobList) {
//							this.masterJobList.add(jobFile);
//						}
//						this.jobList.clear();
//						this.masterResultList = new ConcurrentLinkedQueue<String>();
//						this.interimFileCount = 0;
////						mas = true;
//						return true;
//					}
//				} else {
//					System.out.println("reset for master");
//					if (this.masterResultList.size() == 1) {
//						System.err.println("sorted entire file");
//						System.exit(1);
//					}
//					for (String jobFile : this.masterResultList) {
//						this.masterJobList.add(jobFile);
//					}
//					this.masterResultList.clear();
//					System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~LEVEL UP ~~~~~~~~~~~~~~~");
//					jobLevel++;
//				}
//			}
//			
//			return false;
//		}
		
		public void jobFetchCombiningFiles(NodeInfo node, String firstFileName) {
			String firstFile = null;
			if (firstFileName != null) {
				firstFile = firstFileName;
			} else if (this.jobList.peek() != null) {
				firstFile = this.jobList.poll();
			}
			
			if (firstFile != null && this.jobList.peek() != null) {
				firstFile = String.format("%s_%s", firstFile,this.jobList.poll());
			}
			System.out.println("jobFetchCombiningFiles"+node.jobFileName);
			node.jobFileName = firstFile;
			node.resultFileName = null;
		}
		
		public boolean getJobFileForNode(NodeInfo node) {
			System.out.println("getjobfilefor node job name : "+node.jobFileName + this.jobList.size());
			if (node != null) {
				if (node.resultFileName != null) {
					if (masterSortLevel) {
						this.masterResultList.add(node.resultFileName);
					} else {
						this.resultList.add(node.resultFileName);
					}
				}
				
				if (node.jobFileName != null) {
					this.interimJobList.remove(node.jobFileName);
				}
				
				if (this.jobList.peek() != null) {
					if (jobLevel == 0) {
						node.jobFileName = this.jobList.poll();
						node.resultFileName = null;						
					} else {
						String firstFile = this.jobList.poll();
						if (this.jobList.peek() != null) {
							firstFile = String.format("%s_%s", firstFile,this.jobList.poll());
							node.jobFileName = firstFile;
							node.resultFileName = null;
						} else {
							this.resultList.add(firstFile);
							resetTheJobForNextLevel();
							if (masterSortLevel) {
								node.jobFileName = null;
								node.resultFileName = null;
							} else {
								jobFetchCombiningFiles(node, null);
							}
						}
					}
				} else {
					System.out.println("resetnextlevel+++++++++++++++++++++++++++++++++++++++    .."+node.nodeID);
					resetTheJobForNextLevel();
					if (masterSortLevel) {
						node.jobFileName = null;
						node.resultFileName = null;
					} else {
						jobFetchCombiningFiles(node, null);
					}
					
					//System.out.println("jobFetchCombiningFiles2: "+node.jobFileName);
				}
				if (node.jobFileName != null)
					this.interimJobList.add(node.jobFileName);
			}
			
			return startMaster;
		}
		
		public void getNextJobForMaster (NodeInfo node) {
			System.out.println("getjobfilefor Master job name : "+node.jobFileName + "result" + node.resultFileName+ " " + this.masterJobList.size());
			if (node != null) {
				if (node.resultFileName != null) {
					this.masterResultList.add(node.resultFileName);
					node.resultFileName = null;
					node.jobFileName = null;
				}
				
				if (this.masterJobList.peek() != null) {
					String firstFile = this.masterJobList.poll();
					if (this.masterJobList.peek() != null) {
						firstFile = String.format("%s_%s", firstFile,this.masterJobList.poll());
						node.jobFileName = firstFile;
						node.resultFileName = null;
					} else {
						this.masterResultList.add(firstFile);
						resetJobForMaster();
						this.getNextJobForMaster(node);
					}
				} else {
					System.out.println("resetnextlevel Master");
					resetJobForMaster();
					if(this.masterResultList.size() !=1 ){
						this.getNextJobForMaster(node);
					}else{
						
						node.resultFileName= null;
						node.jobFileName = null;
					}
				}
			}
		}
		
		public void nodeDisconnectError(NodeInfo node, String error) {
			if (node != null && node.jobFileName != null) {
				if (this.interimFileCount > 0 && jobLevel == 1) {
					// Sort the 0th level unsorted files at the Master.
					System.out.println("Sort file at master for 0th level");
					int [] unsortedList = retrieveJobFileForMasterSort(node.jobFileName);
					MasterSort sortFile = new MasterSort(unsortedList,JobManager.getInstance(),node.jobFileName);
					sortFile.start();
				} else {
					String [] nameList;
					nameList = retrieveJobFileNames(node.jobFileName);
					this.interimJobList.remove(node.jobFileName);
					if (nameList.length >= 1) {
						this.jobList.add(nameList[0]);
					}
					if (nameList.length >= 2) {
						this.jobList.add(nameList[1]);
					}
				}
			}
			
			System.out.println("Error : "+error+" at node "+node.nodeID);
		}
	}
	
	public int [] retrieveJobFileForMasterSort(String jobFileName) {
		BufferedReader br = null;
		ArrayList<String> unsortedData = new ArrayList<String>();
		try { 
			String sCurrentLine; 
			File file = new File(this.parentString,jobFileName);
			br = new BufferedReader(new FileReader(file)); 
			while ((sCurrentLine = br.readLine()) != null) {
				unsortedData.add(sCurrentLine);
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
		
		int[] unsortedList  = new int[unsortedData.size()];
		for(int i=0;i<unsortedData.size();i++){
			unsortedList[i] = Integer.parseInt(unsortedData.get(i));
		}
		
		return unsortedList;
	}
	
	public String [] retrieveJobFileNames (String fileName) {
		if (fileName != null) {
			String [] str = fileName.split("\\_");
			return str;
		}
		
		return null;
	}
	
	public synchronized void masterSortOnSuccess(int [] sortList, String jobFileName) {
		String resultFileName = String.format("%s%s.txt", RESULT_FILENAME, 
				jobFileName.replaceAll("\\D+",""));
		String resultFilePath = String.format("%s/%s", this.parentString, resultFileName);
		try (BufferedWriter outputWriter = 
				new BufferedWriter(new FileWriter(resultFilePath));) {
			for (int i = 0; i < sortList.length; i++) {
				outputWriter.newLine();
				outputWriter.write(Integer.toString(sortList[i]));
			}
			outputWriter.flush();  
        }catch (IOException e) {
            System.out.println("Error writing file");
            e.printStackTrace();
        }
		
		this.tracker.interimJobList.remove(jobFileName);
		this.tracker.resultList.add(resultFileName);
		this.tracker.interimFileCount--;
	}
	
	public boolean getNextFileForNode (NodeInfo node, File resultFile) {
		if (node != null) {
			return this.tracker.getJobFileForNode(node);
			
//			File file = null;
//			NodeInfo returnNode = null;
//			if (this.jobLevel == 0) {
//				if (node.resultFileName != null) {
//					file = new File(this.parentString, node.resultFileName);
//					// save to disk
//					file = null;
//					
//					file = new File(this.parentString,node.jobFileName);
//					file.delete();
//					file = null;
//				}
//				
//				this.tracker.getJobFileForNode(node);
//				System.out.println("jobFile : "+node.jobFileName);
//				if (returnNode.jobFileName != null) {
//					try {
//						file = new File (this.parentString, returnNode.jobFileName);
//						System.out.println("jobFile"+file);
//						return file;
//					} catch (NullPointerException e) {
//						System.out.println("failed opening job file for node"+returnNode.nodeID);
//						e.printStackTrace();
//					}
//				}
//			} else {
//				String [] nameList;
//				nameList = retrieveJobFileNames(node.jobFileName);
//				
//					file = new File(this.parentString, node.resultFileName);
//					// save to disk
//					file = null;
//					
//					if (nameList.length >= 1) {
//						file  =  new File(this.parentString,nameList[0]);
//						file.delete();
//						file = null;
//					}
//					if (nameList.length >= 2) {
//						file  =  new File(this.parentString,nameList[1]);
//						file.delete();
//						file = null;
//					}
//					this.tracker.getJobFileForNode(node);
//					if (returnNode.jobFileName != null) {
//						nameList = retrieveJobFileNames(returnNode.jobFileName);
//						ArrayList<File>fileList = new ArrayList<File>();
//						if (nameList.length >= 1) {
//							file  =  new File(this.parentString,nameList[0]);
//							fileList.add(file);
//							file = null;
//						}
//						if (nameList.length >= 2) {
//							file  =  new File(this.parentString,nameList[1]);
//							fileList.add(file);
//							file = null;
//						}
//						return fileList;
//					}
//			}
		}
		
		return false;
	}
	
	public void getNextJobFilesForMaster (NodeInfo node) {
		this.tracker.getNextJobForMaster(node);
	}
	
	public void nodeFailureWithError (NodeInfo node, String error) {
		this.tracker.nodeDisconnectError(node, error);
	}
	
/*	public void splitFile(File f) throws IOException {
		System.out.println("entering split file");
        int splitCounter = 1;

        int sizeOfFiles = 1024 * BLOCK_SIZE; // 200kb
        byte[] buffer = new byte[sizeOfFiles];
         
        try (BufferedInputStream bis = new BufferedInputStream(
                new FileInputStream(f))) {
            //String name = this.getFileNameWithoutExtension(f.getName());

            int tmp = 0; // chunk size
            while ((tmp = bis.read(buffer)) > 0) {
                //File newFile = new File("f.txt");
            	String fileName = String.format("%s%03d.txt", JOB_FILENAME,splitCounter++);
            	File newFile = new File(f.getParent(),fileName);
                try (FileOutputStream out = new FileOutputStream(newFile)) {
                    out.write(buffer, 0, tmp);
                    this.tracker.jobList.add(fileName);
                    this.parentString = f.getParent();
                }
            }
        }
    }*/
//public void splitFile(File f1){
//    	
//    	System.out.println("start");
//    	
//    	PrintWriter bw = null;
//    	int max = LINES_READ;
//    	int count =0;
//    	int splitCounter = 1;
//    	
//    	try
//    	{
//    		String fileName = String.format("%s%03d.txt","job",splitCounter++);
//    		File first = new File(fileName);
//    		BufferedReader br = new BufferedReader(new FileReader(f1));
//    	    bw = new PrintWriter(new FileWriter(first, true));
//    	    Scanner sc1=new Scanner(br);
//    	    String line = sc1.nextLine();
//    	    this.tracker.jobList.add(fileName);
//    	    while (line != null) { 
//    	    	//Thread.sleep(500);
//    	    	count++;
//    	    	//System.out.println("line numer "+count+ " text "+line);
//    	    	
//    	    	if(count == max){
//    	    	//	System.out.println("reached 15 lines time to create new file");
//    	    		bw.write(line);    	    		
//    	    		count =0;
//    	    		if(sc1.hasNext()){
//	    	    		String fileName2 = new String(String.format("%s%03d.txt","job",splitCounter++));
//	    	    		this.tracker.jobList.add(fileName2);
//	    	    		File sec = new File(fileName2);
//	    	    		bw.flush();            
//	    	    	    bw.close();
//	    	    		bw = new PrintWriter(new FileWriter(sec, true));
//	    	    		line = sc1.nextLine();
//    	    		}
//    	    		/*bw.write(line);
//    	        	line = br.readLine();*/
//    	    	}else{
//    	    		
//    	    		if(!sc1.hasNext()){
//    	    			bw.write(line);
//    	    			break;
//    	    		}else{
//	    	    		bw.write(line+"\n");
//	    	    		line = sc1.nextLine();
//    	        	}    
//    	    		
//    	        }
//    	    }
//    	   
//    	    br.close();
//    	    br = null;
//    	    bw.flush();            
//    	    bw.close();
//    	    bw = null;
//    	     
//    	}
//    	catch(Exception e) {
//    	    System.out.println("Exception caught: "+e.getMessage());
//    	} 
//    	System.out.println("done");
//    }
	
	public void splitFile(File f) {
		//File f = new File("test.txt");

		Integer splitCounter = 0;
		int BUF_SIZE = LINES_READ * CHARACTERS_PER_LINE;
		int tmp = BUF_SIZE;
		int skip = 0;

		try {
			System.out.println("Start");
			while(tmp == BUF_SIZE) {
				FileReader fr;
				FileWriter wr;
				String fileName = String.format("%s%03d.txt","job",splitCounter++);
				this.tracker.jobList.add(fileName);
				File fw = new File(fileName);
				fr = new FileReader(f);
				wr = new FileWriter(fw);
				char[] buffer = new char[BUF_SIZE];

				fr.skip(skip);
				tmp = fr.read(buffer, 0, buffer.length);

				if (buffer[buffer.length - 1] == '\n') {
					//System.out.println("inside if ");
					skip += tmp;
					wr.write(buffer);
				} else {
					//System.out.println("inside else ");
					int index = new String(buffer).lastIndexOf("\n");
					skip += index + 1;

					if (index > 0) {
						wr.write(buffer, 0, index);
					} else {
						wr.write(buffer);
					}
				}
				wr.flush();
				wr.close();
				fr.close();
			}
			System.out.println("done");
		} catch (FileNotFoundException e) {
			System.out.println("File not found");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("i/o exception");
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			JobManager manager = new JobManager();
			manager.splitFile(new File("sample_dataset.txt"));
		} catch (Exception e) {
			System.out.println("failed opening file");
			e.printStackTrace();
		}
	}

}
