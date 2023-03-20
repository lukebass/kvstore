package com.s62023080.CPEN431.G4;

import java.util.concurrent.ConcurrentSkipListMap;

public class Logger {
    private final int pid;
    private final int port;

    public Logger(int pid, int port) {
        this.pid = pid;
        this.port = port;
    }

    public void log(String message) {
        System.out.println("Log: " + System.currentTimeMillis());
        System.out.println("PID: " + pid);
        System.out.println("Port: " + port);
        System.out.println(message);
    }

    public void log(String message, int memory) {
        System.out.println("Log: " + System.currentTimeMillis());
        System.out.println("PID: " + pid);
        System.out.println("Port: " + port);
        System.out.println("Memory: " + memory);
        System.out.println(message);
    }

    public void logTables(ConcurrentSkipListMap<Integer, int[]> tables) {
        this.log("Table Size: " + tables.size());
        for (int node : tables.keySet()) {
            System.out.println("Virtual Node: " + node);
            for (int finger : tables.get(node)) {
                System.out.println("Finger Node: " + finger);
            }
        }
    }
}
