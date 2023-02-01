package com.s62023080.CPEN431.A4;

import ca.NetSysLab.ProtocolBuffers.Message.Msg;
import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.HashMap;

public class Server extends Thread {
    private final DatagramSocket socket;

    private HashMap<ByteBuffer, HashMap<Integer, byte[]>> store;

    public Server(String port) throws SocketException {
        socket = new DatagramSocket(Integer.parseInt(port));
    }

    public byte[] receive() throws IOException {
        DatagramPacket responsePacket = new DatagramPacket(new byte[16000], 16000);
        socket.receive(responsePacket);
        ByteBuffer buffer = ByteBuffer.wrap(responsePacket.getData());
        byte[] response = new byte[responsePacket.getLength()];
        buffer.get(response);
        return response;
    }

    public void run() {
        boolean running = true;

        while (running) {
            try {
                Msg reqMsg = Msg.parseFrom(receive());

                // Ensure checksum is valid
                if (reqMsg.getCheckSum() != Utils.createCheckSum(reqMsg.getMessageID().toByteArray(), reqMsg.getPayload().toByteArray())) {
                    continue;
                }

                KVRequest kvRequest = KVRequest.parseFrom(reqMsg.getPayload());
                Msg.Builder resMsg = Msg.newBuilder();
                resMsg.setMessageID(reqMsg.getMessageID());
                KVResponse.Builder kvResponse = KVResponse.newBuilder();

                switch(kvRequest.getCommand()) {
                    case 1:
                        // Put
                        break;
                    case 2:
                        // Get
                        break;
                    case 3:
                        // Remove
                        break;
                    case 4:
                        kvResponse.setErrCode(0);
                        resMsg.setPayload(ByteString.copyFrom(kvResponse.build().toByteArray()));
                        resMsg.setCheckSum(Utils.createCheckSum(resMsg.getMessageID().toByteArray(), resMsg.getPayload().toByteArray()));
                        System.exit(0);
                        break;
                    case 5:
                        // Clear
                        break;
                    case 6:
                        kvResponse.setErrCode(0);
                        resMsg.setPayload(ByteString.copyFrom(kvResponse.build().toByteArray()));
                        resMsg.setCheckSum(Utils.createCheckSum(resMsg.getMessageID().toByteArray(), resMsg.getPayload().toByteArray()));
                        break;
                    case 7:
                        byte[] pid = new byte[8];
                        ByteBuffer buffer = ByteBuffer.wrap(pid);
                        buffer.putLong(ProcessHandle.current().pid());
                        kvResponse.setErrCode(0);
                        kvResponse.setValue(ByteString.copyFrom(pid));
                        resMsg.setPayload(ByteString.copyFrom(kvResponse.build().toByteArray()));
                        resMsg.setCheckSum(Utils.createCheckSum(resMsg.getMessageID().toByteArray(), resMsg.getPayload().toByteArray()));
                        break;
                    case 8:
                        kvResponse.setErrCode(0);
                        kvResponse.setValue(ByteString.copyFrom(new byte[]{1}));
                        resMsg.setPayload(ByteString.copyFrom(kvResponse.build().toByteArray()));
                        resMsg.setCheckSum(Utils.createCheckSum(resMsg.getMessageID().toByteArray(), resMsg.getPayload().toByteArray()));
                        break;
                }
            } catch (IOException e) {
                e.printStackTrace();
                running = false;
            }
        }

        socket.close();
    }
}
