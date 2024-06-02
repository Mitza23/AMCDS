package org.example.communication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class MessageSender {

    private static final Logger log = LoggerFactory.getLogger(MessageSender.class);

    public static void send(CommunicationProtocol.Message message, String receiverHost, int receiverPort) {
        log.info("Sent {} to {}", message.getNetworkMessage().getMessage().getType(), receiverPort);

        byte[] serializedMessage = message.toByteArray();

        try (Socket socket = new Socket(receiverHost, receiverPort);
             DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        ) {
            dataOutputStream.writeInt(serializedMessage.length);
            dataOutputStream.write(serializedMessage);
        } catch (IOException e) {
//            e.printStackTrace();
        }
    }
}
