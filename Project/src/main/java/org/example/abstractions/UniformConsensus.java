package org.example.abstractions;


import org.example.communication.CommunicationProtocol;
import org.example.process.Process;
import org.example.util.AbstractionIdUtil;
import org.example.util.ProcessUtil;
import org.example.util.ValueUtil;

public class UniformConsensus extends Abstraction {
    private CommunicationProtocol.Value val;
    private boolean proposed;
    private boolean decided;
    private int ets;
    private CommunicationProtocol.ProcessId leader;
    private int newts;
    private CommunicationProtocol.ProcessId newLeader;

    public UniformConsensus(String abstractionId, Process process) {
        super(abstractionId, process);

        val = ValueUtil.buildUndefinedValue();
        proposed = false;
        decided = false;

        CommunicationProtocol.ProcessId leader0 = ProcessUtil.getMaxRankedProcess(process.getProcesses());
        ets = 0;
        leader = leader0;
        newts = 0;
        newLeader = null;

        process.registerAbstraction(new EpochChange(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.EC), process));
        process.registerAbstraction(new EpochConsensus(AbstractionIdUtil.getNamedAbstractionId(abstractionId, AbstractionType.EP, "0"), process,
                0, leader0, new EpState(0, ValueUtil.buildUndefinedValue())));
    }

    @Override
    public boolean handle(CommunicationProtocol.Message message) {
        switch (message.getType()) {
            case UC_PROPOSE:
                val = message.getUcPropose().getValue();
                performCheck();
                return true;
            case EC_START_EPOCH:
                newts = message.getEcStartEpoch().getNewTimestamp();
                newLeader = message.getEcStartEpoch().getNewLeader();
                triggerEpEtsAbort();
                return true;
            case EP_ABORTED:
                if (message.getEpAborted().getEts() == ets) {
                    ets = newts;
                    leader = newLeader;
                    proposed = false;
                    process.registerAbstraction(new EpochConsensus(AbstractionIdUtil.getNamedAbstractionId(abstractionId, AbstractionType.EP, Integer.toString(ets)), process,
                            ets, leader, new EpState(message.getEpAborted().getValueTimestamp(), message.getEpAborted().getValue())));
                    performCheck();
                    return true;
                }
                return false;
            case EP_DECIDE:
                if (message.getEpDecide().getEts() == ets) {
                    if (!decided) {
                        decided = true;
                        triggerUcDecide(message.getEpDecide().getValue());
                    }
                    return true;
                }
                return false;
            default:
                return false;
        }
    }

    private void triggerEpEtsAbort() {
        CommunicationProtocol.EpAbort epAbort = CommunicationProtocol.EpAbort
                .newBuilder()
                .build();

        CommunicationProtocol.Message epAbortMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.EP_ABORT)
                .setEpAbort(epAbort)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getNamedAbstractionId(this.abstractionId, AbstractionType.EP, Integer.toString(ets)))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(epAbortMessage);
    }

    private void performCheck() {
        if (leader.equals(process.getProcess()) && val.getDefined() && !proposed) {
            proposed = true;

            CommunicationProtocol.EpPropose epPropose = CommunicationProtocol.EpPropose
                    .newBuilder()
                    .setValue(val)
                    .build();

            CommunicationProtocol.Message epProposeMessage = CommunicationProtocol.Message
                    .newBuilder()
                    .setType(CommunicationProtocol.Message.Type.EP_PROPOSE)
                    .setEpPropose(epPropose)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(AbstractionIdUtil.getNamedAbstractionId(this.abstractionId, AbstractionType.EP, Integer.toString(ets)))
                    .setSystemId(process.getSystemId())
                    .build();

            process.addMessageToQueue(epProposeMessage);
        }
    }

    private void triggerUcDecide(CommunicationProtocol.Value value) {
        CommunicationProtocol.UcDecide ucDecide = CommunicationProtocol.UcDecide
                .newBuilder()
                .setValue(value)
                .build();

        CommunicationProtocol.Message ucDecideMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.UC_DECIDE)
                .setUcDecide(ucDecide)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(ucDecideMessage);
    }
}
