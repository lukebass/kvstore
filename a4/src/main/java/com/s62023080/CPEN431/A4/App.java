package com.s62023080.CPEN431.A4;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {
        try {
            new Server(3080).start();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
