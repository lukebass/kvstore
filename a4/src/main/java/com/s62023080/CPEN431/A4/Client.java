package com.s62023080.CPEN431.A4;

import java.io.IOException;

public class Client {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("This requires an address, port, and student ID");
            return;
        }

        try {
            Fetch fetch = new Fetch(args[0], args[1], 100, 4);
            fetch.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}