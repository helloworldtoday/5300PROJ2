import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Scanner;

public class RemoveReject {
    public static double fromNetID = 0.77;
    public static double rejectMin = 0.9 * fromNetID;
    public static double rejectLimit = rejectMin + 0.01;
	
    public static void main(String[] args) {
        String filename = "/Users/shaoke/Dropbox/5300proj2/data_original.txt";
        removeLine(filename);
    }
	
    public static void removeLine(String fileName) {
        try {
            File file = new File(fileName);
            Scanner scanner = new Scanner(file);
	          PrintWriter writer = new PrintWriter("/Users/shaoke/Dropbox/5300proj2/data_remove.txt", "UTF-8");
            while (scanner.hasNextLine()) {
                // format: random number, source, destination
                String[] substring = scanner.nextLine().split(" ");

                if (selectInputLine(Double.parseDouble(substring[0]))) {
                    String stayLine = substring[1] + " " + substring[2];
                    writer.println(stayLine);
                    System.out.println(stayLine);
                }
            }
            scanner.close();
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
	    e.printStackTrace();
	}
    }
    
    public static boolean selectInputLine(double x) {
        return (((x >= rejectMin) && (x < rejectLimit)) ? false : true);
    }
}
