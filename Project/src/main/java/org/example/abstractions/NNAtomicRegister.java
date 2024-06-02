package org.example.abstractions;


import org.example.communication.CommunicationProtocol;
import org.example.process.Process;
import org.example.util.AbstractionIdUtil;
import org.example.util.ValueUtil;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NNAtomicRegister extends Abstraction {

    private NNARValue nnarValue;
    private int acks;
    private CommunicationProtocol.Value writeVal;
    private int readId;
    private Map<String, NNARValue> readList;
    private CommunicationProtocol.Value readVal;
    private boolean isReading;

    public NNAtomicRegister(String abstractionId, Process process) {
        super(abstractionId, process);
        nnarValue = new NNARValue();
        acks = 0;
        writeVal = ValueUtil.buildUndefinedValue();
        readId = 0;
        readList = new ConcurrentHashMap<>();
        readVal = ValueUtil.buildUndefinedValue();
        isReading = false;

        process.registerAbstraction(new BestEffortBroadcast(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(CommunicationProtocol.Message message) {
        switch (message.getType()) {
            case NNAR_READ:
                handleNnarRead();
                return true;
            case NNAR_WRITE:
                handleNnarWrite(message.getNnarWrite());
                return true;
            case BEB_DELIVER:
                CommunicationProtocol.BebDeliver bebDeliver = message.getBebDeliver();
                switch (bebDeliver.getMessage().getType()) {
                    case NNAR_INTERNAL_READ:
                        CommunicationProtocol.NnarInternalRead nnarInternalRead = bebDeliver.getMessage().getNnarInternalRead();
                        handleBebDeliverInternalRead(bebDeliver.getSender(), nnarInternalRead.getReadId());
                        return true;
                    case NNAR_INTERNAL_WRITE:
                        CommunicationProtocol.NnarInternalWrite nnarInternalWrite = bebDeliver.getMessage().getNnarInternalWrite();
                        NNARValue value = new NNARValue(nnarInternalWrite.getTimestamp(), nnarInternalWrite.getWriterRank(), nnarInternalWrite.getValue());
                        handleBebDeliverInternalWrite(bebDeliver.getSender(), nnarInternalWrite.getReadId(), value);
                        return true;
                    default:
                        return false;
                }
            case PL_DELIVER:
                CommunicationProtocol.PlDeliver plDeliver = message.getPlDeliver();
                switch (plDeliver.getMessage().getType()) {
                    case NNAR_INTERNAL_VALUE:
                        CommunicationProtocol.NnarInternalValue nnarInternalValue = plDeliver.getMessage().getNnarInternalValue();
                        if (nnarInternalValue.getReadId() == this.readId) {
                            NNARValue value = new NNARValue(nnarInternalValue.getTimestamp(), nnarInternalValue.getWriterRank(), nnarInternalValue.getValue());
                            triggerPlDeliverValue(plDeliver.getSender(), nnarInternalValue.getReadId(), value);
                            return true;
                        }
                        return false;
                    case NNAR_INTERNAL_ACK:
                        CommunicationProtocol.NnarInternalAck nnarInternalAck = plDeliver.getMessage().getNnarInternalAck();
                        if (nnarInternalAck.getReadId() == this.readId) {
                            triggerPlDeliverAck(plDeliver.getSender(), nnarInternalAck.getReadId());
                            return true;
                        }
                        return false;
                    default:
                        return false;
                }
            default:
                return false;
        }
    }

    private void handleNnarRead() {
        readId++;
        acks = 0;
        readList = new ConcurrentHashMap<>();
        isReading = true;

        CommunicationProtocol.NnarInternalRead nnarInternalRead = CommunicationProtocol.NnarInternalRead
                .newBuilder()
                .setReadId(readId)
                .build();

        CommunicationProtocol.Message nnarInternalReadMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_READ)
                .setNnarInternalRead(nnarInternalRead)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        triggerBebBroadcast(nnarInternalReadMessage);
    }

    private void handleNnarWrite(CommunicationProtocol.NnarWrite nnarWrite) {
        readId++;
        writeVal = CommunicationProtocol.Value
                .newBuilder()
                .setV(nnarWrite.getValue().getV())
                .setDefined(true)
                .build();
        acks = 0;
        readList = new ConcurrentHashMap<>();

        CommunicationProtocol.NnarInternalRead nnarInternalRead = CommunicationProtocol.NnarInternalRead
                .newBuilder()
                .setReadId(readId)
                .build();

        CommunicationProtocol.Message nnarInternalReadMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_READ)
                .setNnarInternalRead(nnarInternalRead)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        triggerBebBroadcast(nnarInternalReadMessage);
    }

    private void handleBebDeliverInternalRead(CommunicationProtocol.ProcessId sender, int incomingReadId) {
        CommunicationProtocol.NnarInternalValue nnarInternalValue = CommunicationProtocol.NnarInternalValue
                .newBuilder()
                .setReadId(incomingReadId)
                .setTimestamp(this.nnarValue.getTimestamp())
                .setWriterRank(this.nnarValue.getWriterRank())
                .setValue(this.nnarValue.getValue())
                .build();

        CommunicationProtocol.Message nnarInternalValueMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_VALUE)
                .setNnarInternalValue(nnarInternalValue)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        CommunicationProtocol.PlSend plSend = CommunicationProtocol.PlSend
                .newBuilder()
                .setDestination(sender)
                .setMessage(nnarInternalValueMessage)
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

    private void handleBebDeliverInternalWrite(CommunicationProtocol.ProcessId sender, int incomingReadId, NNARValue incomingVal) {
        if (incomingVal.getTimestamp() > nnarValue.getTimestamp() || (incomingVal.getTimestamp() == nnarValue.getTimestamp() && incomingVal.getWriterRank() > nnarValue.getWriterRank())) {
            nnarValue = incomingVal;
        }

        CommunicationProtocol.NnarInternalAck nnarInternalAck = CommunicationProtocol.NnarInternalAck
                .newBuilder()
                .setReadId(incomingReadId)
                .build();

        CommunicationProtocol.Message nnarInternalAckMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_ACK)
                .setNnarInternalAck(nnarInternalAck)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        CommunicationProtocol.PlSend plSend = CommunicationProtocol.PlSend
                .newBuilder()
                .setDestination(sender)
                .setMessage(nnarInternalAckMessage)
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

    private void triggerPlDeliverValue(CommunicationProtocol.ProcessId sender, int incomingReadId, NNARValue incomingValue) {
        String senderId = sender.getOwner() + sender.getIndex();
        this.readList.put(senderId, incomingValue);
        if (this.readList.size() > (process.getProcesses().size() / 2)) {
            NNARValue highestValue = getHighestNnarValue();
            readVal = highestValue.getValue();
            readList.clear();
            CommunicationProtocol.NnarInternalWrite nnarInternalWrite;
            if (isReading) {
                nnarInternalWrite = CommunicationProtocol.NnarInternalWrite
                        .newBuilder()
                        .setReadId(incomingReadId)
                        .setTimestamp(highestValue.getTimestamp())
                        .setWriterRank(highestValue.getWriterRank())
                        .setValue(highestValue.getValue())
                        .build();
            } else {
                nnarInternalWrite = CommunicationProtocol.NnarInternalWrite
                        .newBuilder()
                        .setReadId(incomingReadId)
                        .setTimestamp(highestValue.getTimestamp() + 1)
                        .setWriterRank(process.getProcess().getRank())
                        .setValue(this.writeVal)
                        .build();
            }

            CommunicationProtocol.Message nnarInternalWriteMessage = CommunicationProtocol.Message
                    .newBuilder()
                    .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_WRITE)
                    .setNnarInternalWrite(nnarInternalWrite)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(this.abstractionId)
                    .setSystemId(process.getSystemId())
                    .build();

            triggerBebBroadcast(nnarInternalWriteMessage);
        }
    }

    private void triggerPlDeliverAck(CommunicationProtocol.ProcessId sender, int incomingReadId) {
        acks++;
        if (acks > (process.getProcesses().size() / 2)) {
            acks = 0;
            if (isReading) {
                isReading = false;
                CommunicationProtocol.NnarReadReturn nnarReadReturn = CommunicationProtocol.NnarReadReturn
                        .newBuilder()
                        .setValue(readVal)
                        .build();

                CommunicationProtocol.Message nnarReadReturnMessage = CommunicationProtocol.Message
                        .newBuilder()
                        .setType(CommunicationProtocol.Message.Type.NNAR_READ_RETURN)
                        .setNnarReadReturn(nnarReadReturn)
                        .setFromAbstractionId(this.abstractionId)
                        .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                        .setSystemId(process.getSystemId())
                        .build();

                process.addMessageToQueue(nnarReadReturnMessage);
            } else {
                CommunicationProtocol.NnarWriteReturn nnarWriteReturn = CommunicationProtocol.NnarWriteReturn
                        .newBuilder()
                        .build();

                CommunicationProtocol.Message nnarWriteReturnMessage = CommunicationProtocol.Message
                        .newBuilder()
                        .setType(CommunicationProtocol.Message.Type.NNAR_WRITE_RETURN)
                        .setNnarWriteReturn(nnarWriteReturn)
                        .setFromAbstractionId(this.abstractionId)
                        .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                        .setSystemId(process.getSystemId())
                        .build();

                process.addMessageToQueue(nnarWriteReturnMessage);
            }
        }
    }

    private void triggerBebBroadcast(CommunicationProtocol.Message message) {
        CommunicationProtocol.BebBroadcast bebBroadcast = CommunicationProtocol.BebBroadcast
                .newBuilder()
                .setMessage(message)
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

    private NNARValue getHighestNnarValue() {
        Comparator<NNARValue> nnarValueComparator = Comparator
                .comparingInt(NNARValue::getTimestamp).reversed()
                .thenComparingInt(NNARValue::getWriterRank).reversed();

        return this.readList.values().stream()
                .sorted(nnarValueComparator)
                .findAny()
                .get();
    }
}
