import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.util.Scanner;

public class CombineOutgoing {
	public static void main(String[] args) {
		String filename = "/Users/shaoke/Dropbox/5300proj2/filtered.txt";
		combineLine(filename);
	}
	
    public static void combineLine(String fileName) {
        try {
        	File file = new File(fileName);
        	Scanner scanner = new Scanner(file);
        	PrintWriter writer = new PrintWriter("/Users/shaoke/Dropbox/5300proj2/data_combine.txt", "UTF-8");
        	int i = 0;
        	double pr0 = 1.0/685228.0;
        	DecimalFormat eight = new DecimalFormat("#0.00000000");
        	String combinedLine = String.valueOf(i) + " " + eight.format(pr0) + " ";
        	while (scanner.hasNextLine()) {
        		// format: source, destination
        		String[] substring = scanner.nextLine().split(" ");
        		if (Integer.parseInt(substring[0]) == i) {
        			combinedLine = combinedLine + substring[1] + ",";
        		} else {
        			writer.println(combinedLine.substring(0, combinedLine.length() - 1));
          		  	System.out.println(combinedLine);
        			i++;
        			combinedLine = String.valueOf(i) + " " + eight.format(pr0) + " ";
        		}
        	}
        	scanner.close();
        	writer.close();
        	System.out.println("OKAY");
        } catch (FileNotFoundException e) {
        	e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    }
}
