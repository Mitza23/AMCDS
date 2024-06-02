package org.example.communication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Observable;

public class MessageReceiver extends Observable implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MessageReceiver.class);

    private final int processPort;

    public MessageReceiver(int processPort) {
        this.processPort = processPort;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(processPort)) {
            log.info("Waiting for requests");
            while (true) {
                receiveMessage(serverSocket);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void receiveMessage(ServerSocket serverSocket) throws IOException {
        try (Socket clientSocket = serverSocket.accept();
             DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
        ) {
            int messageSize = dataInputStream.readInt();
            byte[] byteBuffer = new byte[messageSize];
            int readMessageSize = dataInputStream.read(byteBuffer, 0, messageSize);

            if (messageSize != readMessageSize) {
                throw new RuntimeException("Network message has incorrect size: expected = " + messageSize + ", actual = " + readMessageSize);
            }

            CommunicationProtocol.Message message = CommunicationProtocol.Message.parseFrom(byteBuffer);

            if (!CommunicationProtocol.Message.Type.NETWORK_MESSAGE.equals(message.getType())) {
                throw new RuntimeException("Network message has incorrect type: expected = " + CommunicationProtocol.Message.Type.NETWORK_MESSAGE + ", actual = " + message.getType());
            }

            log.info("Received {} from {}", message.getNetworkMessage().getMessage().getType(), message.getNetworkMessage().getSenderListeningPort());

            setChanged();
            notifyObservers(message);
        }
    }
}
