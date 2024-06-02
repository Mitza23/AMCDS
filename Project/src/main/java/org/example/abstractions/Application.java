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
                return handlePlDeliver(plDeliver.getMessage());
            case BEB_DELIVER:
                CommunicationProtocol.Message innerMessage = message.getBebDeliver().getMessage();
                triggerPlSend(innerMessage);
                return true;
            case NNAR_READ_RETURN:
                handleNnarReadReturn(message.getNnarReadReturn(), message.getFromAbstractionId());
                return true;
            case NNAR_WRITE_RETURN:
                handleNnarWriteReturn(message.getNnarWriteReturn(), message.getFromAbstractionId());
                return true;
        }
        return false;
    }

    private boolean handlePlDeliver(CommunicationProtocol.Message message) {
        switch (message.getType()) {
            case APP_BROADCAST:
                handleAppBroadcast(message.getAppBroadcast());
                return true;
            case APP_READ:
                handleAppRead(message.getAppRead());
                return true;
            case APP_WRITE:
                handleAppWrite(message.getAppWrite());
                return true;
        }
        return false;
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

    private void handleAppRead(CommunicationProtocol.AppRead appRead) {
        String nnarAbstractionId = AbstractionIdUtil.getNamedAbstractionId(this.abstractionId, AbstractionType.NNAR, appRead.getRegister());
        process.registerAbstraction(new NNAtomicRegister(nnarAbstractionId, process));

        CommunicationProtocol.NnarRead nnarRead = CommunicationProtocol.NnarRead
                .newBuilder()
                .build();

        CommunicationProtocol.Message nnarReadMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.NNAR_READ)
                .setNnarRead(nnarRead)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(nnarAbstractionId)
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(nnarReadMessage);
    }

    private void handleAppWrite(CommunicationProtocol.AppWrite appWrite) {
        // register app.nnar[register] abstraction if not present
        String nnarAbstractionId = AbstractionIdUtil.getNamedAbstractionId(this.abstractionId, AbstractionType.NNAR, appWrite.getRegister());
        process.registerAbstraction(new NNAtomicRegister(nnarAbstractionId, process));

        CommunicationProtocol.NnarWrite nnarWrite = CommunicationProtocol.NnarWrite
                .newBuilder()
                .setValue(appWrite.getValue())
                .build();

        CommunicationProtocol.Message nnarWriteMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.NNAR_WRITE)
                .setNnarWrite(nnarWrite)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(nnarAbstractionId)
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(nnarWriteMessage);
    }


    private void handleNnarReadReturn(CommunicationProtocol.NnarReadReturn nnarReadReturn, String fromAbstractionId) {
        CommunicationProtocol.AppReadReturn appReadReturn = CommunicationProtocol.AppReadReturn
                .newBuilder()
                .setRegister(AbstractionIdUtil.getInternalNameFromAbstractionId(fromAbstractionId))
                .setValue(nnarReadReturn.getValue())
                .build();

        CommunicationProtocol.Message appReadReturnMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.APP_READ_RETURN)
                .setAppReadReturn(appReadReturn)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.HUB_ID)
                .setSystemId(process.getSystemId())
                .build();

        triggerPlSend(appReadReturnMessage);
    }

    private void handleNnarWriteReturn(CommunicationProtocol.NnarWriteReturn nnarWriteReturn, String fromAbstractionId) {
        CommunicationProtocol.AppWriteReturn appWriteReturn = CommunicationProtocol.AppWriteReturn
                .newBuilder()
                .setRegister(AbstractionIdUtil.getInternalNameFromAbstractionId(fromAbstractionId))
                .build();

        CommunicationProtocol.Message appWriteReturnMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.APP_WRITE_RETURN)
                .setAppWriteReturn(appWriteReturn)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.HUB_ID)
                .setSystemId(process.getSystemId())
                .build();

        triggerPlSend(appWriteReturnMessage);
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
