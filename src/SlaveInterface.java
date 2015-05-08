
import java.io.File;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;

public interface SlaveInterface extends Remote {
	public File sendFile(Object f, String MethodType, NodeInfo n) throws RemoteException;
	
	public void readFile(NodeInfo node) throws RemoteException;
	public void sendFile(NodeInfo node, byte[] data, int len) throws RemoteException;
	public void doneReadingFile(boolean merge) throws RemoteException;
	
	public HashSet<Integer> getUniqueData() throws RemoteException;
}
