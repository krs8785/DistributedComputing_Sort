import java.io.Serializable;


public class NodeInfo implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3L;
	public String nodeID;
	public String jobFileName;
	public String resultFileName;
	public boolean isBusy;
	
	
	
	public NodeInfo() {
		this.nodeID = null;
		this.jobFileName = null;
		this.resultFileName = null;
		this.isBusy = false;
	}
	public String getNodeID() {
		return nodeID;
	}
	public void setNodeID(String nodeID) {
		this.nodeID = nodeID;
	}
	public String getJobFileName() {
		return jobFileName;
	}
	public void setJobFileName(String jobFileName) {
		this.jobFileName = jobFileName;
	}
	public String getResultFileName() {
		return resultFileName;
	}
	public void setResultFileName(String resultFileName) {
		this.resultFileName = resultFileName;
	}
	public boolean isBusy() {
		return isBusy;
	}
	public void setBusy(boolean busy) {
		this.isBusy = busy;
	}
	public String toString(){
		return "nodeID:"+nodeID +" :: "+ jobFileName +" :: "+ resultFileName +" :: "+ isBusy;
	}
}
