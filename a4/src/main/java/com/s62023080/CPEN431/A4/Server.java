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

    private final static int MEMORY_ERROR = 2;

    private final static int OVERLOAD_ERROR = 3;

    private final static int STORE_ERROR = 4;

    private final static int UNRECOGNIZED_ERROR = 5;

    private final static int INVALID_KEY_ERROR = 6;

    private final static int INVALID_VALUE_ERROR = 7;

    public Server(int port) throws SocketException {
        this.socket = new DatagramSocket(port);
        this.store = new Store();
    }

    public void run() {
        boolean running = true;

        while (running) {
            DatagramPacket reqPacket = new DatagramPacket(new byte[16000], 16000);
            Msg.Builder resMsg = Msg.newBuilder();
            KVResponse.Builder kvResponse = KVResponse.newBuilder();

            try {
                this.socket.receive(reqPacket);
                ByteBuffer buffer = ByteBuffer.wrap(reqPacket.getData());
                byte[] request = new byte[reqPacket.getLength()];
                buffer.get(request);
                Msg reqMsg = Msg.parseFrom(request);
                resMsg.setMessageID(reqMsg.getMessageID());

                // Ensure checksum is valid
                if (reqMsg.getCheckSum() != Utils.createCheckSum(reqMsg.getMessageID().toByteArray(), reqMsg.getPayload().toByteArray())) {
                    continue;
                }

                KVRequest kvRequest = KVRequest.parseFrom(reqMsg.getPayload());
                switch (kvRequest.getCommand()) {
                    // Put
                    case 1 -> {
                        if (kvRequest.getKey().size() > 32) {
                            kvResponse.setErrCode(INVALID_KEY_ERROR);
                        } else if (kvRequest.getValue().size() > 10000) {
                            kvResponse.setErrCode(INVALID_VALUE_ERROR);
                        } else {
                            this.store.put(kvRequest.getKey().toByteArray(), kvRequest.getValue().toByteArray(), kvRequest.getVersion());
                        }
                    }
                    // Get
                    case 2 -> {
                        if (kvRequest.getKey().size() > 32) {
                            kvResponse.setErrCode(INVALID_KEY_ERROR);
                        } else {
                            byte[] composite = this.store.get(kvRequest.getKey().toByteArray());
                            if (composite == null) {
                                kvResponse.setErrCode(MISSING_KEY_ERROR);
                            } else {
                                kvResponse.setErrCode(SUCCESS);
                                buffer = ByteBuffer.wrap(composite);
                                int version = buffer.getInt();
                                byte[] value = new byte[composite.length - 4];
                                buffer.get(value);
                                kvResponse.setValue(ByteString.copyFrom(value));
                                kvResponse.setVersion(version);
                            }
                        }
                    }
                    // Remove
                    case 3 -> {
                        if (kvRequest.getKey().size() > 32) {
                            kvResponse.setErrCode(INVALID_KEY_ERROR);
                        } else {
                            byte[] composite = this.store.remove(kvRequest.getKey().toByteArray());
                            if (composite == null) {
                                kvResponse.setErrCode(MISSING_KEY_ERROR);
                            } else {
                                kvResponse.setErrCode(SUCCESS);
                            }
                        }
                    }
                    // Shutdown
                    case 4 -> {
                        running = false;
                        kvResponse.setErrCode(SUCCESS);
                    }
                    // Clear
                    case 5 -> {
                        this.store.clear();
                        kvResponse.setErrCode(SUCCESS);
                    }
                    // Health
                    case 6 -> kvResponse.setErrCode(SUCCESS);
                    // PID
                    case 7 -> {
                        byte[] pid = new byte[8];
                        buffer = ByteBuffer.wrap(pid);
                        buffer.putLong(ProcessHandle.current().pid());
                        kvResponse.setErrCode(SUCCESS);
                        kvResponse.setValue(ByteString.copyFrom(pid));
                    }
                    // Membership Count
                    case 8 -> {
                        byte[] count = new byte[4];
                        buffer = ByteBuffer.wrap(count);
                        buffer.putInt(1);
                        kvResponse.setErrCode(SUCCESS);
                        kvResponse.setValue(ByteString.copyFrom(count));
                    }
                    // Unknown
                    default -> kvResponse.setErrCode(UNRECOGNIZED_ERROR);
                }
            } catch (OutOfMemoryError e) {
                kvResponse.setErrCode(MEMORY_ERROR);
            } catch (IOException e) {
                kvResponse.setErrCode(STORE_ERROR);
            } catch (Exception e) {
                byte[] wait = new byte[4];
                ByteBuffer buffer = ByteBuffer.wrap(wait);
                buffer.putInt(5000);
                kvResponse.setErrCode(OVERLOAD_ERROR);
                kvResponse.setValue(ByteString.copyFrom(wait));
            } finally {
                try {
                    resMsg.setPayload(ByteString.copyFrom(kvResponse.build().toByteArray()));
                    resMsg.setCheckSum(Utils.createCheckSum(resMsg.getMessageID().toByteArray(), resMsg.getPayload().toByteArray()));
                    byte[] response = resMsg.build().toByteArray();
                    DatagramPacket resPacket = new DatagramPacket(response, response.length, reqPacket.getAddress(), reqPacket.getPort());
                    this.socket.send(resPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        this.socket.close();
        System.exit(0);
    }
}
