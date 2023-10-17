import java.util.regex.Matcher;
import java.util.regex.Pattern;


/*CS550 Advanced Operating Systems Programming Assignment 1 Repo
Illinois Institute of Technology

Team Name: KK Students:

Anirudha Kapileshwari (akapileshwari@hawk.iit.edu)
Mugdha Atul Kulkarni (mkulkarni2@hawk.iit.edu) */

// This class if the provided IP address is correct/valid

public class IPAV {
    private static final String IPADDRESS_PATTERN =
        "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
      + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
      + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
      + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

  
    public static boolean validate(final String ipAddress) {
        Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
        Matcher matcher = pattern.matcher(ipAddress);
        return matcher.matches();
    }
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    public static void main(String[] args) {
        String ipAddress = "192.168.64.6";

        if (validate(ipAddress)) {
            System.out.println(ipAddress + " is a valid IP address.");
        } else {
            System.out.println(ipAddress + " is not a valid IP address.");
        }
    }
}

//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++