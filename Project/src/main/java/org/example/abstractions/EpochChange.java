package org.example.abstractions;

import org.example.communication.CommunicationProtocol;
import org.example.process.Process;
import org.example.util.AbstractionIdUtil;
import org.example.util.ProcessUtil;

public class EpochChange extends Abstraction {

    private CommunicationProtocol.ProcessId trusted;
    private int lastTimestamp;
    private int timestamp;

    public EpochChange(String abstractionId, Process process) {
        super(abstractionId, process);
        trusted = ProcessUtil.getMaxRankedProcess(process.getProcesses());
        lastTimestamp = 0;
        timestamp = process.getProcess().getRank();

        process.registerAbstraction(new EventualLeaderDetector(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.ELD), process));
        process.registerAbstraction(new BestEffortBroadcast(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(CommunicationProtocol.Message message) {
        switch (message.getType()) {
            case ELD_TRUST:
                handleEldTrust(message.getEldTrust().getProcess());
                return true;
            case BEB_DELIVER:
                CommunicationProtocol.BebDeliver bebDeliver = message.getBebDeliver();
                if (bebDeliver.getMessage().getType() == CommunicationProtocol.Message.Type.EC_INTERNAL_NEW_EPOCH) {
                    handleBebDeliverNewEpoch(bebDeliver.getSender(), bebDeliver.getMessage().getEcInternalNewEpoch().getTimestamp());
                    return true;
                }
                return false;
            case PL_DELIVER:
                CommunicationProtocol.PlDeliver plDeliver = message.getPlDeliver();
                if (plDeliver.getMessage().getType() == CommunicationProtocol.Message.Type.EC_INTERNAL_NACK) {
                    handleNack();
                    return true;
                }
                return false;
            default:
                return false;
        }
    }

    private void handleEldTrust(CommunicationProtocol.ProcessId p) {
        trusted = p;
        if (p.equals(process.getProcess())) {
            timestamp += process.getProcesses().size();
            triggerBebBroadcastNewEpoch();
        }
    }

    private void handleBebDeliverNewEpoch(CommunicationProtocol.ProcessId sender, int newTimestamp) {
        if (sender.equals(trusted) && newTimestamp > lastTimestamp) {
            lastTimestamp = newTimestamp;
            CommunicationProtocol.EcStartEpoch startEpoch = CommunicationProtocol.EcStartEpoch
                    .newBuilder()
                    .setNewLeader(sender)
                    .setNewTimestamp(newTimestamp)
                    .build();

            CommunicationProtocol.Message startEpochMessage = CommunicationProtocol.Message
                    .newBuilder()
                    .setType(CommunicationProtocol.Message.Type.EC_START_EPOCH)
                    .setEcStartEpoch(startEpoch)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                    .setSystemId(process.getSystemId())
                    .build();

            process.addMessageToQueue(startEpochMessage);
        } else {
            CommunicationProtocol.EcInternalNack ecNack = CommunicationProtocol.EcInternalNack
                    .newBuilder()
                    .build();

            CommunicationProtocol.Message nackMessage = CommunicationProtocol.Message
                    .newBuilder()
                    .setType(CommunicationProtocol.Message.Type.EC_INTERNAL_NACK)
                    .setEcInternalNack(ecNack)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(this.abstractionId)
                    .setSystemId(process.getSystemId())
                    .build();

            CommunicationProtocol.PlSend plSend = CommunicationProtocol.PlSend
                    .newBuilder()
                    .setDestination(sender)
                    .setMessage(nackMessage)
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

    private void handleNack() {
        if (trusted.equals(process.getProcess())) {
            timestamp += process.getProcesses().size();
            triggerBebBroadcastNewEpoch();
        }
    }

    private void triggerBebBroadcastNewEpoch() {
        CommunicationProtocol.EcInternalNewEpoch newEpoch = CommunicationProtocol.EcInternalNewEpoch
                .newBuilder()
                .setTimestamp(timestamp)
                .build();

        CommunicationProtocol.Message newEpochMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.EC_INTERNAL_NEW_EPOCH)
                .setEcInternalNewEpoch(newEpoch)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        CommunicationProtocol.BebBroadcast bebBroadcast = CommunicationProtocol.BebBroadcast
                .newBuilder()
                .setMessage(newEpochMessage)
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
