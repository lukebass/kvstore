package com.s62023080.CPEN431.A4;

import ca.NetSysLab.ProtocolBuffers.Message.Msg;
import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;

public class Server extends Thread {
    private final DatagramSocket socket;

    private final Store store;

    private final static int SUCCESS = 0;

    private final static int MISSING_KEY_ERROR = 1;

    private final static int SPACE_ERROR = 2;

    private final static int STORE_ERROR = 4;

    private final static int UNRECOGNIZED_ERROR = 5;

    private final static int INVALID_KEY_ERROR = 6;

    private final static int INVALID_VALUE_ERROR = 7;

    public Server(String port) throws SocketException {
        socket = new DatagramSocket(Integer.parseInt(port));
        store = new Store();
    }

    public void run() {
        boolean running = true;

        while (running) {
            try {
                DatagramPacket requestPacket = new DatagramPacket(new byte[16000], 16000);
                socket.receive(requestPacket);
                ByteBuffer buffer = ByteBuffer.wrap(requestPacket.getData());
                byte[] request = new byte[requestPacket.getLength()];
                buffer.get(request);
                Msg reqMsg = Msg.parseFrom(request);

                // Ensure checksum is valid
                if (reqMsg.getCheckSum() != Utils.createCheckSum(reqMsg.getMessageID().toByteArray(), reqMsg.getPayload().toByteArray())) {
                    continue;
                }

                KVRequest kvRequest = KVRequest.parseFrom(reqMsg.getPayload());
                KVResponse.Builder kvResponse = KVResponse.newBuilder();

                switch (kvRequest.getCommand()) {
                    case 1 -> {
                        // Put
                        if (kvRequest.getKey().size() > 32) {
                            kvResponse.setErrCode(INVALID_KEY_ERROR);
                        } else if (kvRequest.getValue().size() > 10000) {
                            kvResponse.setErrCode(INVALID_VALUE_ERROR);
                        } else {
                            store.put(kvRequest.getKey().toByteArray(), kvRequest.getValue().toByteArray(), kvRequest.getVersion());
                        }
                    }
                    case 2 -> {
                        // Get
                        if (kvRequest.getKey().size() > 32) {
                            kvResponse.setErrCode(INVALID_KEY_ERROR);
                        } else {
                            byte[] value = store.get(kvRequest.getKey().toByteArray());
                        }
                    }
                    case 3 -> {
                        // Remove
                        if (kvRequest.getKey().size() > 32) {
                            kvResponse.setErrCode(INVALID_KEY_ERROR);
                        } else {
                            byte[] removed = store.remove(kvRequest.getKey().toByteArray());
                        }
                    }
                    case 4 -> {
                        // Shutdown
                        kvResponse.setErrCode(SUCCESS);
                        System.exit(0);
                    }
                    case 5 -> {
                        // Clear
                        store.clear();
                        kvResponse.setErrCode(SUCCESS);
                    }
                    case 6 ->
                        // Health
                            kvResponse.setErrCode(SUCCESS);
                    case 7 -> {
                        // PID
                        byte[] pid = new byte[8];
                        buffer = ByteBuffer.wrap(pid);
                        buffer.putLong(ProcessHandle.current().pid());
                        kvResponse.setErrCode(SUCCESS);
                        kvResponse.setValue(ByteString.copyFrom(pid));
                    }
                    case 8 -> {
                        // Membership Count
                        byte[] count = new byte[4];
                        buffer = ByteBuffer.wrap(count);
                        buffer.putInt(1);
                        kvResponse.setErrCode(SUCCESS);
                        kvResponse.setValue(ByteString.copyFrom(count));
                    }
                    default -> {
                        // Unknown
                        kvResponse.setErrCode(UNRECOGNIZED_ERROR);
                    }
                }

                Msg.Builder resMsg = Msg.newBuilder();
                resMsg.setMessageID(reqMsg.getMessageID());
                resMsg.setPayload(ByteString.copyFrom(kvResponse.build().toByteArray()));
                resMsg.setCheckSum(Utils.createCheckSum(resMsg.getMessageID().toByteArray(), resMsg.getPayload().toByteArray()));
                // Return response
            } catch (IOException e) {
                e.printStackTrace();
                running = false;
            }
        }

        socket.close();
    }
}
