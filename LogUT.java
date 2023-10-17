
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.io.BufferedWriter;
import java.io.File;
import java.util.Calendar;
import java.io.FileWriter;




// log handling class

public class LogUT {

	private String logF = "";
	private final String logloc = "logs/";
	private BufferedWriter writer = null;
	

	

	public LogUT(String logType) {
		try {
			String logF = getLogFileName(logType);  // Get the log file name based on logType
			createLogsFolder();  // Create a "logs" folder if it doesn't exist
	
			writer = new BufferedWriter(new FileWriter(logloc + logF, true));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private String getLogFileName(String logType) {
		switch (logType.toLowerCase()) {
			case "peer":
				return "peer.server.log";
			case "server":
				return "server.log";
			case "replication":
				return "replication.log";
			default:
				throw new IllegalArgumentException("Invalid logType: " + logType);
		}
	}
	
	private void createLogsFolder() {
		File file = new File(logloc);
		if (!file.exists()) {
			if (file.mkdir()) {
				System.out.println("Logs folder created.");
			} else {
				System.err.println("Failed to create logs folder.");
			}
		}
	}
	

	


	public boolean write(String logText) {
		try {
			if (writer != null) {
				String timestamp = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").format(Calendar.getInstance().getTime());
				String formattedLog = String.format("%s => %s%n", timestamp, logText);
				writer.write(formattedLog);
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	
	// Print method
	public void print() {
		
		
		int charCount = 0;
		
		System.out.println("\nLOG");
		System.out.println("=========================================================================");
		
		
		if (charCount == 0) {
			System.out.println("NO LOGS TO PRINT");
		}

		System.out.println("=========================================================================");
	}
	
	// close the log
	public void closelog() {
		try {
			if (writer != null) {
				String newline = System.getProperty("line.separator");
				writer.write(newline);
				writer.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//destroy
	@Override
	protected void finalize() throws Throwable {
		if (writer != null) {
			writer.close();
		}
		
	}
	
	
}