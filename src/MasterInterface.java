
import java.io.File;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MasterInterface extends Remote {
	
	public void SlaveUP(SlaveInterface slave, NodeInfo node) throws RemoteException;
	
	public void receiveFileAfterSort(File f, NodeInfo n) throws RemoteException;
	
	public void readFile(NodeInfo node) throws RemoteException;
	
	public void sendFile(NodeInfo node, byte[] data, int len) throws RemoteException;

}
