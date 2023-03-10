package com.s62023080.CPEN431.A4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class App
{
    /**
     * @param args args[0] server.jar; args[1] servers.txt
     */
    public static void main(String[] args)
    {
        if(args.length != 2) {
            System.out.println("Must provide server.jar and servers.txt");
            System.exit(1);
        }

        try {
            BufferedReader reader = new BufferedReader(new FileReader(args[1]));
            String line = reader.readLine();
            while (line != null) {
                // java -Xmx64m -jar server-jar-with-dependencies.jar servers port threads weight
                ProcessBuilder pb = new ProcessBuilder(
                        "java",
                        "-Xmx64m",
                        "-jar",
                        System.getProperty("user.dir") + "/" + args[0],
                        System.getProperty("user.dir") + "/" + args[1],
                        line.split(":")[1],
                        "1",
                        "5"
                );
                String filename = line.split(":")[1];
                File output = new File(System.getProperty("user.dir") + "/" + filename + ".log");
                System.out.println(output.getName());
                pb.redirectOutput(output);
                pb.start();
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
