import java.net.URL;
import java.net.URLConnection;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

public class WebPageScanner {	
    public static void main(String[] args) {
        try {
            URLConnection connection = new URL("http://edu-cornell-cs-cs5300s15-proj2.s3.amazonaws.com/edges.txt").openConnection();
            Scanner scanner = new Scanner(connection.getInputStream()).useDelimiter(" +");
            PrintWriter writer = new PrintWriter("/Users/shaoke/Dropbox/5300proj2/data_original.txt", "UTF-8");
            int i = 0;
            while (scanner.hasNext()) {
                String text = scanner.next();
                System.out.println(text);
                if (i % 3 == 2) {
                    writer.print(text);
                } else {
                    writer.print(text + " ");
                }
            i++;
        }
        writer.close();
        System.out.println("OKAY");
        } catch (IOException e) {
        e.printStackTrace();
        }
    }
}
