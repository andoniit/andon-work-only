import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.swing.plaf.synth.SynthSplitPaneUI;

/***
 * This class provides the Replication facility in the Distributed Hash Table.
 * Its serves the requests of peers to store their Hash Table data on the replication nodes.
 */
public class replication extends Thread {
	private ArrayList<String> replicationNodes = null;
	private int portAddress;
	private String localAddress = null;
	private HashMap<Integer, String> networkMap = null;
	
	private String key = null;
	private String value = null;
	private String requestType = null;
	
	// Initialize all the local data from the global data
	public replication(String key, String value, String requestType) {
		replicationNodes = FileTS.getRNodes();
		portAddress = FileTS.getpSPort();
		localAddress = FileTS.getLocAddr();
		networkMap = FileTS.getNetWM();
		
		this.key = key;
		this.value = value;
		this.requestType = requestType;
	}
	
	public void run () {
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		REQ peerRequest = null;
		RES serverResponse = null;
		
		String data = key + "," + value;
		
		if (requestType.equalsIgnoreCase("REPLICATE")) {
			replicateHashTables();
			replicateFiles();
			this.interrupt();
		}
		
		for (String nodeAddress : replicationNodes) {
			try {
				if (requestType.equalsIgnoreCase("REGISTER")) {
					if (nodeAddress.equalsIgnoreCase(localAddress)) {
						//System.out.println(String.format("\nREPLICATING If (%s,%s) at %s - %s", key, value, nodeAddress, localAddress));
						LogUT log = new LogUT("peer");
						log.write(String.format("Serving REPLICATE - REGISTER(%s,%s) request of %s.", key, value, localAddress));
						FileTS.putinREP_HT(nodeAddress, key, value);
						replicate(value, portAddress, key);
						log.write(String.format("REPLICATE - REGISTER(%s,%s) for %s completed successfully.", key, value, localAddress));
						//System.out.println(nodeAddress + " # " + value);
						log.closelog();
					} else {
						//System.out.println(String.format("\nREPLICATING Else (%s,%s) at %s - %s", key, value, nodeAddress, localAddress));
						
						// Make connection with server using the specified Host Address and Port 10000
						socket = new Socket(nodeAddress, portAddress);

						// Initializing output stream using the socket's output stream
						out = new ObjectOutputStream(socket.getOutputStream());
						out.flush();

						// Initializing input stream using the socket's input stream
						in = new ObjectInputStream(socket.getInputStream());

						// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
						peerRequest = new REQ();
						peerRequest.setRequestType("R_REGISTER");
						peerRequest.setRequestData(data);
						out.writeObject(peerRequest);

						// Read the response message from the server
						serverResponse = (RES) in.readObject();
						socket.close();
					}
					//System.out.println(replicatedHashTable);
				} else if (requestType.equalsIgnoreCase("UNREGISTER")) {
					// Make connection with server using the specified Host Address and Port 10000
			        socket = new Socket(nodeAddress, portAddress);
			        
			        // Initializing output stream using the socket's output stream
			        out = new ObjectOutputStream(socket.getOutputStream());
			        out.flush();
			        
			        // Initializing input stream using the socket's input stream
			        in = new ObjectInputStream(socket.getInputStream());

					// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
					peerRequest = new REQ();
					peerRequest.setRequestType("R_UNREGISTER");
					peerRequest.setRequestData(key);
					out.writeObject(peerRequest);
					
			        // Read the response message from the server
			        serverResponse = (RES) in.readObject();
			        socket.close();
				}
			} catch (Exception ex) {
				// e.printStackTrace();
			} finally {
				try {
					// Closing all streams. Close the stream only if it is initialized
					if (out != null)
						out.close();

					if (in != null)
						in.close();

					if (socket != null)
						socket.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
		}
		this.interrupt();
	}
	
	/***
	 * This method retrieves the Hash Table from all the peers in the network and adds it to its Replication Hash Table.
	 * @return	Returns true if all Hash Tables were retrieved successfully.
	 */
	private boolean replicateHashTables() {
		ConcurrentHashMap<String, HashMap<String, String>> replicatedHashTable = null;		
		if (replicationNodes.size() > 1) {
			// If there is another replication node, get replication data from another data
			replicatedHashTable = getReplicationData();
		} else {
			// If there is only one replication node. It gets data from all the peers in the network.
			replicatedHashTable = getAllHashTables();
		}
		
		if (replicatedHashTable != null && replicatedHashTable.size() > 0) {
			FileTS.setReplicatedHT(replicatedHashTable);
			return true;
		}
		return false;
	}
	
	private void replicateFiles() {
		for (Map.Entry peer : FileTS.getReplicatedHT().entrySet()) {
			//String peerAddress = peer.getKey().toString();
			HashMap<String, String> hashTable = (HashMap<String, String>) peer.getValue();
			
			for (Map.Entry record : hashTable.entrySet()) {
				// Replicate file from the respective peer
				String fileName = record.getKey().toString();
				String peerAddress = record.getValue().toString();
				//System.out.println(peerAddress + " # " + fileName);
				replicate(peerAddress, portAddress, fileName);
			}
		}
		this.interrupt();
	}
	
	/***
	 * This method is used to download the file from the requested Peer.
	 * @param hostAddress 	IP Address of the peer used to download the file
	 * @param port			Port of the per used to download the file
	 * @param fileName		Name of the file to be downloaded
	 */
	private void replicate(String hostAddress, int port, String fileName) {
		FileUT.replicateFile(hostAddress, port, fileName);
	}
	
	/***
	 * This method retrieves the Replication Hash Table from one of the replication nodes after this peer was down so that it can re-gain its replication data from the replication nodes.
	 * @return	Returns the replicatedHashTable (if exists) of the calling peer from the replication nodes.
	 */
	private ConcurrentHashMap<String, HashMap<String, String>> getReplicationData() {
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		REQ peerRequest = null;
		RES serverResponse	= null;
		ConcurrentHashMap<String, HashMap<String, String>> hm = null;
		
		for (String nodeAddress : replicationNodes) {
			//System.out.println("Retrieving Replication Hash Table from " + nodeAddress);
			
			if (nodeAddress.equalsIgnoreCase(localAddress)) {
				continue;
			}
			
			try {
				// Make connection with server using the specified Host Address and Port 10000
				socket = new Socket(nodeAddress, portAddress);

				// Initializing output stream using the socket's output stream
				out = new ObjectOutputStream(socket.getOutputStream());
				out.flush();

				// Initializing input stream using the socket's input stream
				in = new ObjectInputStream(socket.getInputStream());

				// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
				peerRequest = new REQ();
				peerRequest.setRequestType("GET_REPLICA");
				out.writeObject(peerRequest);

				// Read the response message from the server
				serverResponse = (RES) in.readObject();
				if (serverResponse != null && serverResponse.getResponseCode() == 200) {
					hm = (ConcurrentHashMap<String, HashMap<String, String>>) serverResponse.getResponseData();
				} 
					
				socket.close();
				break;
			} catch (Exception ex) {
				//ex.printStackTrace();
			} finally {
				try {
					// Closing all streams. Close the stream only if it is initialized
					if (out != null)
						out.close();

					if (in != null)
						in.close();

					if (socket != null)
						socket.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
		}

		return hm;
	}

	/***
	 * This method requests all the peers in the network to send their hash tables.
	 * @return Returns the combined hash table of all the peers in the network.
	 */
	private ConcurrentHashMap<String, HashMap<String, String>> getAllHashTables() {
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		REQ peerRequest = null;
		RES serverResponse = null;
		ConcurrentHashMap<String, HashMap<String, String>> replicatedHashTable = new ConcurrentHashMap<String, HashMap<String, String>>();
		ConcurrentHashMap<String, String> hm = null;
		
		for (Map.Entry<Integer, String> peer : networkMap.entrySet()) {
			if (peer.getValue().equalsIgnoreCase(localAddress)) {
				if (FileTS.getHT().size() > 0) {
					replicatedHashTable.put(peer.getValue(), new HashMap<String, String>(FileTS.getHT()));
				}
				continue;
			}
			
			try {
				// Make connection with server using the specified Host Address and Port portAddress
				socket = new Socket(peer.getValue(), portAddress);

				// Initializing output stream using the socket's output stream
				out = new ObjectOutputStream(socket.getOutputStream());
				out.flush();

				// Initializing input stream using the socket's input stream
				in = new ObjectInputStream(socket.getInputStream());

				// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
				peerRequest = new REQ();
				peerRequest.setRequestType("GET_HASHTABLE");
				out.writeObject(peerRequest);

				// Read the response message from the server
				serverResponse = (RES) in.readObject();
				if (serverResponse != null && serverResponse.getResponseCode() == 200) {
					hm = (ConcurrentHashMap<String, String>) serverResponse.getResponseData();
				}
				socket.close();
				socket = null;
				
			} catch (Exception e) {
				//e.printStackTrace();
			} finally {
				try {
					// Closing all streams. Close the stream only if it is initialized
					if (out != null)
						out.close();

					if (in != null)
						in.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
			
			if (hm != null && hm.size() > 0) {
				replicatedHashTable.put(peer.getValue(), new HashMap<String, String>(hm));
				hm = null;
			}
		}
		
		return replicatedHashTable;
	}
}