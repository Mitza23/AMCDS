package org.example.abstractions;


import org.example.communication.CommunicationProtocol;
import org.example.communication.MessageSender;
import org.example.process.Process;
import org.example.util.AbstractionIdUtil;

import java.util.Optional;
import java.util.UUID;

public class PerfectLink extends Abstraction {

    public PerfectLink(String abstractionId, Process process) {
        super(abstractionId, process);
    }

    // PerfectLink can:
    // 1. send a message to a desired Process -> send Message(NetworkMessage(PlSendMessage.Message))
    // 2. deliver a message from a sender to the parent abstraction -> add Message(PlDeliverMessage(NetworkMessage.Message)) to the queue
    @Override
    public boolean handle(CommunicationProtocol.Message message) {
        switch (message.getType()) {
            case PL_SEND:
                handlePlSend(message.getPlSend(), message.getToAbstractionId());
                return true;
            case NETWORK_MESSAGE:
                triggerPlDeliver(message.getNetworkMessage(), AbstractionIdUtil.getParentAbstractionId(message.getToAbstractionId()));
                return true;
        }
        return false;
    }

    private void handlePlSend(CommunicationProtocol.PlSend plSendMessage, String toAbstractionId) {
        CommunicationProtocol.ProcessId sender = process.getProcess();
        CommunicationProtocol.ProcessId destination = plSendMessage.getDestination();

        CommunicationProtocol.NetworkMessage networkMessage = CommunicationProtocol.NetworkMessage
                .newBuilder()
                .setSenderHost(sender.getHost())
                .setSenderListeningPort(sender.getPort())
                .setMessage(plSendMessage.getMessage())
                .build();

        CommunicationProtocol.Message outerMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(networkMessage)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(toAbstractionId)
                .setSystemId(process.getSystemId())
                .setMessageUuid(UUID.randomUUID().toString())
                .build();

        MessageSender.send(outerMessage, destination.getHost(), destination.getPort());
    }


    private void triggerPlDeliver(CommunicationProtocol.NetworkMessage networkMessage, String toAbstractionId) {
        Optional<CommunicationProtocol.ProcessId> sender = process.getProcessByHostAndPort(networkMessage.getSenderHost(), networkMessage.getSenderListeningPort());
        CommunicationProtocol.PlDeliver.Builder plDeliverBuilder = CommunicationProtocol.PlDeliver
                .newBuilder()
                .setMessage(networkMessage.getMessage());
        sender.ifPresent(plDeliverBuilder::setSender);

        CommunicationProtocol.PlDeliver plDeliver = plDeliverBuilder.build();

        CommunicationProtocol.Message message = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.PL_DELIVER)
                .setPlDeliver(plDeliver)
                .setToAbstractionId(toAbstractionId)
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(message);
    }
}
