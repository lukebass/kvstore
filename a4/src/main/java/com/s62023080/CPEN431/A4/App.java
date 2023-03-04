package com.s62023080.CPEN431.A4;

public class App 
{
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("This requires port, number of threads, cache expiration (ms), wait time (ms)");
            return;
        }

        try {
            System.out.println("Server running on port: " + args[0]);
            System.out.println("Number of threads: " + args[1]);
            System.out.println("Cache expiration (ms): " + args[2]);
            System.out.println("Wait time (ms): " + args[3]);
            new Server(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
