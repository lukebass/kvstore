package com.s62023080.CPEN431.A4;

public class App 
{
    public static void main(String[] args) {
        try {
            new Server(3080, 1000).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
