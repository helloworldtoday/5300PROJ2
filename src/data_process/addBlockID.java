import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.util.Scanner;

public class addBlockID {
    public static void main(String[] args) {
        String filename = "/Users/shaoke/Dropbox/5300proj2/data_combine.txt";
        addID(filename);
    }
	
    public static void addID(String fileName) {
        try {
            String blockFileName = "/Users/shaoke/Dropbox/5300proj2/blocks.txt";
            File blockFile = new File(blockFileName);
            Scanner scannerBlock = new Scanner(blockFile);
          int[] group = new int[68];
          int j = 0;
          while (scannerBlock.hasNextLine()) {
          group[j++] = Integer.parseInt(scannerBlock.nextLine().trim());
        }
        scannerBlock.close();
        	
        for (int i = 1; i < group.length; i++) {
            group[i] += group[i - 1];
        }
        	
//        	/**
        File file = new File(fileName);
        Scanner scanner = new Scanner(file);
        PrintWriter writer = new PrintWriter("/Users/shaoke/Dropbox/5300proj2/blockAdded.txt", "UTF-8");
        int k = 0; // k: block #
        while (scanner.hasNextLine()) {
            String oldLine = scanner.nextLine();
            String[] substring = oldLine.split(" ");
            if (Integer.parseInt(substring[0]) == group[k]) {
                k++;
            } 
            String newLine = k + " " + oldLine;
            writer.println(newLine);
            System.out.println(newLine);
        }
        scanner.close();
        writer.close();
//        	**/
        System.out.println("OKAY");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
