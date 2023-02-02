package com.s62023080.CPEN431.A4;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {
        if (args.length != 1) {
            System.out.println("This requires a port number");
            return;
        }

        try {
            new Server(Integer.parseInt(args[0])).start();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
