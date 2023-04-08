package com.s62023080.CPEN431.G4;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListMap;

public class Logger {
    private final int pid;
    private final int port;

    public Logger(int pid, int port) {
        this.pid = pid;
        this.port = port;
    }

    public void log(String message) {
        System.out.println(message);
        System.out.println("Log: " + System.currentTimeMillis());
        System.out.println("PID: " + pid);
        System.out.println("Port: " + port);
        System.out.println("\n");
    }

    public void log(String message, int memory) {
        System.out.println(message);
        System.out.println("Log: " + System.currentTimeMillis());
        System.out.println("PID: " + pid);
        System.out.println("Port: " + port);
        System.out.println("Memory: " + memory);
        System.out.println("\n");
    }

    public void log(String message, ArrayList<Integer> replicas) {
        System.out.println(message);
        System.out.println("Replicas: " + replicas.toString());
        System.out.println("Log: " + System.currentTimeMillis());
        System.out.println("PID: " + pid);
        System.out.println("Port: " + port);
        System.out.println("\n");
    }

    public void log(ConcurrentSkipListMap<Integer, Integer> addresses) {
        System.out.println("Addresses Size: " + addresses.size());
        for (int nodeID : addresses.keySet()) {
            System.out.println("ID: " + nodeID);
            System.out.println("Port: " + addresses.get(nodeID));
        }
        System.out.println("\n");
    }
}
