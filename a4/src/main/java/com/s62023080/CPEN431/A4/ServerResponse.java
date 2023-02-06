package com.s62023080.CPEN431.A4;

import ca.NetSysLab.ProtocolBuffers.Message.Msg;
import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;
import com.google.common.cache.Cache;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.net.*;

public class ServerResponse implements Runnable {
    private final DatagramSocket socket;

    private final DatagramPacket packet;

    private final Store store;

    private final Cache<ByteString, byte[]> cache;

    private final static int SUCCESS = 0;

    private final static int MISSING_KEY_ERROR = 1;

    private final static int MEMORY_ERROR = 2;

    private final static int OVERLOAD_ERROR = 3;

    private final static int STORE_ERROR = 4;

    private final static int UNRECOGNIZED_ERROR = 5;

    private final static int INVALID_KEY_ERROR = 6;

    private final static int INVALID_VALUE_ERROR = 7;

    public ServerResponse(DatagramSocket socket, DatagramPacket packet, Store store, Cache<ByteString, byte[]> cache) throws SocketException {
        this.socket = socket;
        this.packet = packet;
        this.store = store;
        this.cache = cache;
    }

    public void run() {
        Msg.Builder resMsg = Msg.newBuilder();
        KVResponse.Builder kvResponse = KVResponse.newBuilder();
        ByteBuffer buffer = ByteBuffer.wrap(this.packet.getData());

        try {
            byte[] request = new byte[this.packet.getLength()];
            buffer.get(request);
            Msg reqMsg = Msg.parseFrom(request);
            resMsg.setMessageID(reqMsg.getMessageID());
            byte[] cacheValue = cache.getIfPresent(reqMsg.getMessageID());

            if (cacheValue != null) {
                DatagramPacket resPacket = new DatagramPacket(cacheValue, cacheValue.length, this.packet.getAddress(), this.packet.getPort());
                this.socket.send(resPacket);
                return;
            } else if (this.cache.size() > Utils.MAX_CACHE_SIZE) {
                throw new IOException("Max cache size");
            }

            KVRequest kvRequest = KVRequest.parseFrom(reqMsg.getPayload());
            switch (kvRequest.getCommand()) {
                // Put
                case 1 -> {
                    if (Utils.isOutOfMemory()) {
                        throw new OutOfMemoryError("Out of memory");
                    } else if (kvRequest.getKey().size() == 0 || kvRequest.getKey().size() > 32) {
                        kvResponse.setErrCode(INVALID_KEY_ERROR);
                    } else if (kvRequest.getValue().size() == 0 || kvRequest.getValue().size() > 10000) {
                        kvResponse.setErrCode(INVALID_VALUE_ERROR);
                    } else {
                        this.store.put(kvRequest.getKey(), kvRequest.getValue().toByteArray(), kvRequest.getVersion());
                        kvResponse.setErrCode(SUCCESS);
                    }
                }
                // Get
                case 2 -> {
                    if (kvRequest.getKey().size() == 0 || kvRequest.getKey().size() > 32) {
                        kvResponse.setErrCode(INVALID_KEY_ERROR);
                    } else {
                        byte[] composite = this.store.get(kvRequest.getKey());
                        if (composite == null) {
                            kvResponse.setErrCode(MISSING_KEY_ERROR);
                        } else {
                            buffer = ByteBuffer.wrap(composite);
                            int version = buffer.getInt();
                            byte[] value = new byte[composite.length - 4];
                            buffer.get(value);
                            kvResponse.setErrCode(SUCCESS);
                            kvResponse.setValue(ByteString.copyFrom(value));
                            kvResponse.setVersion(version);
                        }
                    }
                }
                // Remove
                case 3 -> {
                    if (kvRequest.getKey().size() == 0 || kvRequest.getKey().size() > 32) {
                        kvResponse.setErrCode(INVALID_KEY_ERROR);
                    } else {
                        byte[] composite = this.store.remove(kvRequest.getKey());
                        if (composite == null) {
                            kvResponse.setErrCode(MISSING_KEY_ERROR);
                        } else {
                            kvResponse.setErrCode(SUCCESS);
                        }
                    }
                }
                // Shutdown
                case 4 -> {
                    this.socket.close();
                    System.exit(0);
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
        } catch (IOException e) {
            kvResponse.setErrCode(OVERLOAD_ERROR);
            kvResponse.setOverloadWaitTime(1000);
        } catch (OutOfMemoryError e) {
            kvResponse.setErrCode(MEMORY_ERROR);
        } catch (Exception e) {
            kvResponse.setErrCode(STORE_ERROR);
        } finally {
            try {
                resMsg.setPayload(ByteString.copyFrom(kvResponse.build().toByteArray()));
                resMsg.setCheckSum(Utils.createCheckSum(resMsg.getMessageID().toByteArray(), resMsg.getPayload().toByteArray()));
                byte[] response = resMsg.build().toByteArray();
                DatagramPacket resPacket = new DatagramPacket(response, response.length, this.packet.getAddress(), this.packet.getPort());
                this.socket.send(resPacket);
                this.cache.put(resMsg.getMessageID(), response);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
