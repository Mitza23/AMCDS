package org.example.abstractions;

import org.example.communication.CommunicationProtocol;
import org.example.process.Process;
import org.example.util.AbstractionIdUtil;
import org.example.util.ProcessUtil;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class EventualLeaderDetector extends Abstraction {

    private Set<CommunicationProtocol.ProcessId> suspected;
    private CommunicationProtocol.ProcessId leader;

    public EventualLeaderDetector(String abstractionId, Process process) {
        super(abstractionId, process);
        suspected = new CopyOnWriteArraySet<>();

        process.registerAbstraction(new EventuallyPerfectFailureDetector(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.EPFD), process));
    }

    @Override
    public boolean handle(CommunicationProtocol.Message message) {
        switch (message.getType()) {
            case EPFD_SUSPECT:
                suspected.add(message.getEpfdSuspect().getProcess());
                performCheck();
                return true;
            case EPFD_RESTORE:
                suspected.remove(message.getEpfdSuspect().getProcess());
                performCheck();
                return true;
            default:
                return false;
        }
    }

    private void performCheck() {
        Set<CommunicationProtocol.ProcessId> notSuspected = new CopyOnWriteArraySet<>(process.getProcesses());
        notSuspected.removeAll(suspected);
        CommunicationProtocol.ProcessId maxRankedProcess = ProcessUtil.getMaxRankedProcess(notSuspected);
        if (maxRankedProcess != null && !maxRankedProcess.equals(leader)) {
            leader = maxRankedProcess;

            CommunicationProtocol.EldTrust eldTrust = CommunicationProtocol.EldTrust
                    .newBuilder()
                    .setProcess(leader)
                    .build();

            CommunicationProtocol.Message trustMessage = CommunicationProtocol.Message
                    .newBuilder()
                    .setType(CommunicationProtocol.Message.Type.ELD_TRUST)
                    .setEldTrust(eldTrust)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                    .setSystemId(process.getSystemId())
                    .build();

            process.addMessageToQueue(trustMessage);
        }
    }
}
