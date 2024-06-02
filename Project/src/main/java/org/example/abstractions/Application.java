package org.example.abstractions;


import org.example.communication.CommunicationProtocol;
import org.example.process.Process;
import org.example.util.AbstractionIdUtil;

public class Application extends Abstraction {

    public Application(String abstractionId, Process process) {
        super(abstractionId, process);
        process.registerAbstraction(new BestEffortBroadcast(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(CommunicationProtocol.Message message) {
        switch (message.getType()) {
            case PL_DELIVER:
                CommunicationProtocol.PlDeliver plDeliver = message.getPlDeliver();
                handlePlDeliver(plDeliver.getMessage());
                return true;
            case BEB_DELIVER:
                CommunicationProtocol.Message innerMessage = message.getBebDeliver().getMessage();
                triggerPlSend(innerMessage);
                return true;
        }
        return false;
    }

    private void handlePlDeliver(CommunicationProtocol.Message message) {
        if (message.getType() == CommunicationProtocol.Message.Type.APP_BROADCAST) {
            handleAppBroadcast(message.getAppBroadcast());
        }
    }

    private void triggerPlSend(CommunicationProtocol.Message message) {
        CommunicationProtocol.PlSend plSend = CommunicationProtocol.PlSend
                .newBuilder()
                .setDestination(process.getHub())
                .setMessage(message)
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
    }

    private void handleAppBroadcast(CommunicationProtocol.AppBroadcast appBroadcast) {
        CommunicationProtocol.AppValue appValue = CommunicationProtocol.AppValue
                .newBuilder()
                .setValue(appBroadcast.getValue())
                .build();

        CommunicationProtocol.Message appValueMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.APP_VALUE)
                .setAppValue(appValue)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        CommunicationProtocol.BebBroadcast bebBroadcast = CommunicationProtocol.BebBroadcast
                .newBuilder()
                .setMessage(appValueMessage)
                .build();

        CommunicationProtocol.Message bebBroadcastMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.BEB_BROADCAST)
                .setBebBroadcast(bebBroadcast)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getChildAbstractionId(this.abstractionId, AbstractionType.BEB))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(bebBroadcastMessage);
    }
}
