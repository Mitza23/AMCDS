package org.example;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServer {
    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server is listening on port " + port);

            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("New client connected");

                try (InputStream input = socket.getInputStream()) {
                    Object message = new ObjectInputStream(input).readObject();
                    handleIncomingMessage(message);
                } catch (IOException | ClassNotFoundException e) {
                    System.out.println("Error reading message: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("Server exception: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void handleIncomingMessage(Object message) {
        // Process the incoming message
        System.out.println("Received message: " + message);
        // Implement your logic here based on the message content
    }
}
