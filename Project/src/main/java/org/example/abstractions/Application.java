package org.example.abstractions;


import org.example.communication.CommunicationProtocol;
import org.example.process.Process;
import org.example.util.AbstractionIdUtil;

public class Application extends Abstraction {

    public Application(String abstractionId, Process process) {
        super(abstractionId, process);
        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(CommunicationProtocol.Message message) {
        if (message.getType() == CommunicationProtocol.Message.Type.PL_DELIVER) {
            CommunicationProtocol.PlDeliver plDeliver = message.getPlDeliver();
        }
        return false;
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
}
