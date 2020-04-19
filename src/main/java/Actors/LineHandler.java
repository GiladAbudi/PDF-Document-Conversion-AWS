package Actors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class LineHandler implements Runnable {
    private String queue,appId,fileName;

    public LineHandler(String workerIQUrl, String appId, String fileName) {
        this.queue=workerIQUrl;
        this.appId=appId;
        this.fileName=fileName;
    }

    public void run() {
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(fileName));
            String line = reader.readLine();
            while (line != null) {
                Manager.handleInputLine(queue, line, appId);
                System.out.println("Manager: sent line " + line + "to workers");
                // read next line
                line = reader.readLine();
            }
            reader.close();
            File f = new File(fileName);
            f.delete();
        }catch(IOException e){
            e.printStackTrace();
            System.out.println("Got Exception while reading input file" + e.getMessage());
        }
    }
}
