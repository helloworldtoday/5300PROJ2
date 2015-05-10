import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Scanner;

public class RandomPartition {
    public static void main(String[] args) {
        String filename = "/Users/shaoke/Dropbox/5300proj2/data/data_simplePR_combined.txt";
        addID(filename);
    }
	
    public static void addID(String fileName) {
        try {
        	int[] group = new int[68];
        	for (int i = 0; i < 68; i++) {
        		group[i] = 0;
        	}
        	File file = new File(fileName);
	        Scanner scanner = new Scanner(file);
	        PrintWriter writer = new PrintWriter("/Users/shaoke/Dropbox/5300proj2/blockAdded.txt", "UTF-8");
	        long k = 0; // k: block #
	        while (scanner.hasNextLine()) {
	            String oldLine = scanner.nextLine();
	            String[] substring = oldLine.split(" ");
	
	            long nodeID = Long.parseLong(substring[0]);
	            k = (3*nodeID + 6) % 68;
	            group[(int) k]++;
	            String newLine = k + " " + oldLine;
	            writer.println(newLine);
	        }
	        scanner.close();
	        writer.close();
	        System.out.println("OKAY");
	        System.out.println(group[0]);
	        for (int j = 1; j < 68; j++) {
	        	group[j] += group[j - 1];
	        	System.out.println(group[j]);
	        }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
