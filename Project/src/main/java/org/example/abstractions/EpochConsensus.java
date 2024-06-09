package org.example.abstractions;

import org.example.communication.CommunicationProtocol;
import org.example.process.Process;
import org.example.util.AbstractionIdUtil;
import org.example.util.ValueUtil;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class EpochConsensus extends Abstraction {

    private int ets;
    private CommunicationProtocol.ProcessId leader;

    private EpState state;
    private CommunicationProtocol.Value tmpVal;
    private Map<CommunicationProtocol.ProcessId, EpState> states;
    private int accepted;
    private boolean halted;

    public EpochConsensus(String abstractionId, Process process, int ets, CommunicationProtocol.ProcessId leader, EpState state) {
        super(abstractionId, process);
        this.ets = ets;
        this.leader = leader;

        this.state = state;
        this.tmpVal = ValueUtil.buildUndefinedValue();
        this.states = new HashMap<>();
        this.accepted = 0;
        this.halted = false;

        process.registerAbstraction(new BestEffortBroadcast(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(CommunicationProtocol.Message message) {
        if (halted) {
            return false;
        }

        switch (message.getType()) {
            case EP_PROPOSE:
                tmpVal = message.getEpPropose().getValue();
                triggerBebBroadcastEpInternalRead();
                return true;
            case BEB_DELIVER:
                CommunicationProtocol.BebDeliver bebDeliver = message.getBebDeliver();
                switch (bebDeliver.getMessage().getType()) {
                    case EP_INTERNAL_READ:
                        triggerPlSendEpState(bebDeliver.getSender());
                        return true;
                    case EP_INTERNAL_WRITE:
                        state = new EpState(ets, bebDeliver.getMessage().getEpInternalWrite().getValue());
                        triggerPlSendEpAccept(bebDeliver.getSender());
                        return true;
                    case EP_INTERNAL_DECIDED:
                        triggerEpDecide(bebDeliver.getMessage().getEpInternalDecided().getValue());
                        return true;
                    default:
                        return false;
                }
            case PL_DELIVER:
                CommunicationProtocol.PlDeliver plDeliver = message.getPlDeliver();
                switch (plDeliver.getMessage().getType()) {
                    case EP_INTERNAL_STATE:
                        CommunicationProtocol.EpInternalState deliveredState = plDeliver.getMessage().getEpInternalState();
                        states.put(plDeliver.getSender(), new EpState(deliveredState.getValueTimestamp(), deliveredState.getValue()));
                        performStatesCheck();
                        return true;
                    case EP_INTERNAL_ACCEPT:
                        accepted++;
                        performAcceptedCheck();
                        return true;
                    default:
                        return false;
                }
            case EP_ABORT:
                triggerEpAborted();
                halted = true;
                return true;
            default:
                return false;
        }
    }

    private void triggerBebBroadcastEpInternalRead() {
        CommunicationProtocol.EpInternalRead epRead = CommunicationProtocol.EpInternalRead
                .newBuilder()
                .build();

        CommunicationProtocol.Message epReadMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_READ)
                .setEpInternalRead(epRead)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        triggerBebBroadcast(epReadMessage);
    }

    private void triggerPlSendEpState(CommunicationProtocol.ProcessId sender) {
        CommunicationProtocol.EpInternalState epState = CommunicationProtocol.EpInternalState
                .newBuilder()
                .setValueTimestamp(state.getValTimestamp())
                .setValue(state.getVal())
                .build();

        CommunicationProtocol.Message epStateMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_STATE)
                .setEpInternalState(epState)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        triggerPlSend(epStateMessage, sender);
    }

    private void triggerPlSendEpAccept(CommunicationProtocol.ProcessId sender) {
        CommunicationProtocol.EpInternalAccept epAccept = CommunicationProtocol.EpInternalAccept
                .newBuilder()
                .build();

        CommunicationProtocol.Message epAcceptMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_ACCEPT)
                .setEpInternalAccept(epAccept)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        triggerPlSend(epAcceptMessage, sender);
    }

    private void performStatesCheck() {
        if (states.size() > process.getProcesses().size() / 2) {
            EpState highest = getHighestState();
            if (highest.getVal().getDefined()) {
                tmpVal = highest.getVal();
            }
            states.clear();

            CommunicationProtocol.EpInternalWrite epWrite = CommunicationProtocol.EpInternalWrite
                    .newBuilder()
                    .setValue(tmpVal)
                    .build();

            CommunicationProtocol.Message epWriteMessage = CommunicationProtocol.Message
                    .newBuilder()
                    .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_WRITE)
                    .setEpInternalWrite(epWrite)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(this.abstractionId)
                    .setSystemId(process.getSystemId())
                    .build();

            triggerBebBroadcast(epWriteMessage);
        }
    }

    private void performAcceptedCheck() {
        if (accepted > process.getProcesses().size() / 2) {
            accepted = 0;

            CommunicationProtocol.EpInternalDecided epDecided = CommunicationProtocol.EpInternalDecided
                    .newBuilder()
                    .setValue(tmpVal)
                    .build();

            CommunicationProtocol.Message epDecidedMessage = CommunicationProtocol.Message
                    .newBuilder()
                    .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_DECIDED)
                    .setEpInternalDecided(epDecided)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(this.abstractionId)
                    .setSystemId(process.getSystemId())
                    .build();

            triggerBebBroadcast(epDecidedMessage);
        }
    }

    private void triggerEpDecide(CommunicationProtocol.Value value) {
        CommunicationProtocol.EpDecide epDecide = CommunicationProtocol.EpDecide
                .newBuilder()
                .setEts(ets)
                .setValue(value)
                .build();

        CommunicationProtocol.Message epDecideMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.EP_DECIDE)
                .setEpDecide(epDecide)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(epDecideMessage);
    }

    private void triggerEpAborted() {
        CommunicationProtocol.EpAborted epAborted = CommunicationProtocol.EpAborted
                .newBuilder()
                .setEts(ets)
                .setValueTimestamp(state.getValTimestamp())
                .setValue(state.getVal())
                .build();

        CommunicationProtocol.Message epAbortedMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.EP_ABORTED)
                .setEpAborted(epAborted)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(epAbortedMessage);
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

    private void triggerPlSend(CommunicationProtocol.Message message, CommunicationProtocol.ProcessId destination) {
        CommunicationProtocol.PlSend plSend = CommunicationProtocol.PlSend
                .newBuilder()
                .setDestination(destination)
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

    private EpState getHighestState() {
        return states.values().stream()
                .max(Comparator.comparing(EpState::getValTimestamp))
                .orElse(new EpState(0, ValueUtil.buildUndefinedValue()));
    }
}
