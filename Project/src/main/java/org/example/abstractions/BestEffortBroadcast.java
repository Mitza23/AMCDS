package org.example.abstractions;


import org.example.communication.CommunicationProtocol;
import org.example.process.Process;
import org.example.util.AbstractionIdUtil;

public class BestEffortBroadcast extends Abstraction {

    public BestEffortBroadcast(String abstractionId, Process process) {
        super(abstractionId, process);
        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(CommunicationProtocol.Message message) {
        switch (message.getType()) {
            case BEB_BROADCAST:
                handleBebBroadcast(message.getBebBroadcast());
                return true;
            case PL_DELIVER:
                triggerBebDeliver(message.getPlDeliver().getMessage(), message.getPlDeliver().getSender());
                return true;
        }
        return false;
    }

    private void handleBebBroadcast(CommunicationProtocol.BebBroadcast bebBroadcast) {
        process.getProcesses().forEach(p -> {
            CommunicationProtocol.PlSend plSend = CommunicationProtocol.PlSend
                    .newBuilder()
                    .setDestination(p)
                    .setMessage(bebBroadcast.getMessage())
                    .build();

            CommunicationProtocol.Message plSendMessage = CommunicationProtocol.Message
                    .newBuilder()
                    .setType(CommunicationProtocol.Message.Type.PL_SEND)
                    .setPlSend(plSend)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(AbstractionIdUtil.getChildAbstractionId(this.abstractionId, AbstractionType.PL))
                    .setSystemId(process.getSystemId())
                    .build();

            process.addMessageToQueue(plSendMessage);
        });
    }

    private void triggerBebDeliver(CommunicationProtocol.Message appValueMessage, CommunicationProtocol.ProcessId sender) {
        CommunicationProtocol.BebDeliver bebDeliver = CommunicationProtocol.BebDeliver
                .newBuilder()
                .setMessage(appValueMessage)
                .setSender(sender)
                .build();

        CommunicationProtocol.Message bebDeliverMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.BEB_DELIVER)
                .setBebDeliver(bebDeliver)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(bebDeliverMessage);
    }
}
