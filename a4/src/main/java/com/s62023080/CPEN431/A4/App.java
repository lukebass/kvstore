package com.s62023080.CPEN431.A4;

public class App 
{
    public static void main(String[] args) {
        try {
            new Server(Integer.parseInt(args[0]), 1).start();
            System.out.println("Server running on port: " + args[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
