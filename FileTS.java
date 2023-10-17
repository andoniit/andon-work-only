
import java.util.Map;
import java.io.IOException;
import java.util.Properties;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.net.ServerSocket;
import java.util.concurrent.ConcurrentHashMap;




// File sharing system



public class FileTS {
	// Creating Hash Table named HT

	private static ConcurrentHashMap<String, String> HT = new ConcurrentHashMap<String, String>();

	// Creating Hash Table named replicatedHT 

	private static ConcurrentHashMap<String, HashMap<String, String>> replicatedHT = new ConcurrentHashMap<String, HashMap<String, String>>();
	
	private static HashMap<Integer, String> netWM = new HashMap<Integer, String>();
	private static ArrayList<String> RNodes = new ArrayList<String>();
	
	
	private static String fileloc = null;
	private static String replicaloc = null;
	
	private static final int P_S_PORT = 3020;
	private static final String LOC_ADDR = networkutil.getLocalAddress();
	
	
	public static boolean putinHT(String key, String value, boolean confirm) {
		if (confirm || !HT.containsKey(key)) {
			HT.put(key, value);
			return true;

		} return false;
		
	}
	
	public static void removefromHT(String key) {
		HT.remove(key);
	}

	public static String getfromHT(String key) {
		return HT.get(key);
	}
	
	
	
	

	public static void putinREP_HT(String nodeAddress, String key, String value) {
		if (replicatedHT.containsKey(nodeAddress)) {
			HashMap<String, String> innerMap = replicatedHT.get(nodeAddress);
			innerMap.put(key, value);
		} else {
			HashMap<String, String> innerMap = new HashMap<String, String>();
			innerMap.put(key, value);
			replicatedHT.put(nodeAddress, innerMap);
		}
	}
	
	public static void removefromREP_HT(String nodeAddress, String key) {
		HashMap<String, String> innerMap = replicatedHT.get(nodeAddress);
		if (innerMap != null) {
			innerMap.remove(key);
		}
	}


	public static String getfromREP_HT(String key) {
		String value = null;
		
		for (Map.Entry<String, HashMap<String, String>> record : replicatedHT.entrySet()) {
			HashMap<String, String> innerMap = replicatedHT.get(record.getKey().toString());
			value = innerMap.get(key);
			if (value != null) {
				break;
			}
		}
		return value;
	}
	
	
	public static void setReplicatedHT(ConcurrentHashMap<String, HashMap<String, String>> replicatedHT) {
		FileTS.replicatedHT = replicatedHT;
	}

	
	// gets replicated hash table

	public static ConcurrentHashMap<String, HashMap<String, String>> getReplicatedHT() {
		return replicatedHT;
	}

	

	public static ConcurrentHashMap<String, String> getHT() {
		return HT;
	}


	public static ArrayList<String> getRNodes() {
		return RNodes;
	}

	
	


	public static void setHT(ConcurrentHashMap<String, String> hashTable) {
		FileTS.HT = hashTable;
	}
	

	public static HashMap<Integer, String> getNetWM() {
		return netWM;
	}

	
	public static int getpSPort() {
		return P_S_PORT;
	}
	
	
	public static String getLocAddr() {
		return LOC_ADDR;
	}
	
	public static void main(String[] args) throws IOException {
		FileInputStream fileStream = null;
		
		try {
			// Load data from n.config files
			Properties config = new Properties();

			
			fileStream = new FileInputStream("n.config");
			config.load(fileStream);
			fileStream.close();
			
			
			// read ip's
			String peerList = config.getProperty("NODES");
			
			if (peerList != null) {
				String[] peers = peerList.split(",");
				
				for (int i = 0; i < peers.length; i++) {
					if (IPAV.validate(peers[i].trim())) {
						netWM.put(i + 1, peers[i].trim());
					} else {
						System.out.println("error due to peer IP Invalid....");
						System.exit(0);
					}					
				}
				
				if (netWM.isEmpty()) {
					System.out.println("Ip's not added...program terminated....");
					System.exit(0);
				}
			} else
			{System.out.println("error in confrigation file"); System.exit(0);}
			
			////////////////
			// fetch data from replicate log ip 

			
			peerList = config.getProperty("REPLICATION_NODES");
			
			if (peerList != null) { 
				String[] peers = peerList.split(",");
				
				for (int i = 0; i < peers.length; i++) 
				{
					if (IPAV.validate(peers[i].trim())) 
					{
					
						RNodes.add(peers[i].trim());


					} 
					else
					{
						System.out.println("error due to replication peer IP Invalid...."); 
						System.exit(0);
					}
					
				}
			}
			
			// sharedd files location
			if (config.getProperty("FILES_LOC") != null) {
				fileloc = config.getProperty("FILES_LOC");
			} fileloc = "files/";
			
			
			// Read Replica Files Location where all files are to be stored for replication purpose
			if (config.getProperty("REPLICA_LOCATION") != null) {
				replicaloc = config.getProperty("REP_NODE");
			} else {
				replicaloc = "replica/";
			}
			
			
		} catch (Exception e) {
			System.out.println("configration file error......");
			
			
			System.exit(0);

		} finally {
			try {
				if (fileStream != null) {
					fileStream.close();
				}
			} catch (Exception e2) { }
		}
		
		// peer and server started 
		System.out.println("Peer Client Started........");
		pc peerClient = new pc();
		peerClient.start();
		
		
		System.out.println("Peer Server Started........");



		ServerSocket ears = new ServerSocket(P_S_PORT);
        try {
            while (true) {
            	ps peerServer = new ps(ears.accept());
               peerServer.start();
            }
        } finally {
            ears.close();
        }
	}
	
	public static String getReplicaloc() {
		return replicaloc;
	}

	public static String getFileloc() {
		return fileloc;
	}

	
}
