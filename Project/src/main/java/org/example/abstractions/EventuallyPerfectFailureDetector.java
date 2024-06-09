package org.example.abstractions;

import org.example.communication.CommunicationProtocol;
import org.example.process.Process;
import org.example.util.AbstractionIdUtil;

import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArraySet;

public class EventuallyPerfectFailureDetector extends Abstraction {

    private static final int DELTA = 100;
    private Set<CommunicationProtocol.ProcessId> alive;
    private Set<CommunicationProtocol.ProcessId> suspected;
    private int delay;

    public EventuallyPerfectFailureDetector(String abstractionId, Process process) {
        super(abstractionId, process);
        alive = new CopyOnWriteArraySet<>(process.getProcesses());
        suspected = new CopyOnWriteArraySet<>();
        delay = DELTA;
        startTimer(delay);

        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(CommunicationProtocol.Message message) {
        switch (message.getType()) {
            case EPFD_TIMEOUT:
                handleEpfdTimeout();
                return true;
            case PL_DELIVER:
                CommunicationProtocol.PlDeliver plDeliver = message.getPlDeliver();
                switch (plDeliver.getMessage().getType()) {
                    case EPFD_INTERNAL_HEARTBEAT_REQUEST:
                        handleHeartbeatRequest(plDeliver.getSender());
                        return true;
                    case EPFD_INTERNAL_HEARTBEAT_REPLY:
                        handleHeartbeatReply(plDeliver.getSender());
                        return true;
                    default:
                        return false;
                }
            default:
                return false;
        }
    }

    private void handleEpfdTimeout() {
        Set<CommunicationProtocol.ProcessId> aliveSuspectIntersection = new CopyOnWriteArraySet<>(alive);
        aliveSuspectIntersection.retainAll(suspected);
        if (!aliveSuspectIntersection.isEmpty()) {
            delay += DELTA;
        }

        process.getProcesses().forEach(p -> {
            if (!alive.contains(p) && !suspected.contains(p)) {
                suspected.add(p);
                triggerSuspect(p);
            } else if (alive.contains(p) && suspected.contains(p)) {
                suspected.remove(p);
                triggerRestore(p);
            }
            triggerPlSendHeartbeatRequest(p);
        });

        alive.clear();
        startTimer(delay);
    }

    private void handleHeartbeatRequest(CommunicationProtocol.ProcessId sender) {
        CommunicationProtocol.EpfdInternalHeartbeatReply epfdHeartbeatReply = CommunicationProtocol.EpfdInternalHeartbeatReply
                .newBuilder()
                .build();

        CommunicationProtocol.Message heartbeatReplyMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.EPFD_INTERNAL_HEARTBEAT_REPLY)
                .setEpfdInternalHeartbeatReply(epfdHeartbeatReply)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        CommunicationProtocol.PlSend plSend = CommunicationProtocol.PlSend
                .newBuilder()
                .setDestination(sender)
                .setMessage(heartbeatReplyMessage)
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

    private void handleHeartbeatReply(CommunicationProtocol.ProcessId sender) {
        alive.add(sender);
    }

    private void triggerSuspect(CommunicationProtocol.ProcessId p) {
        CommunicationProtocol.EpfdSuspect epfdSuspect = CommunicationProtocol.EpfdSuspect
                .newBuilder()
                .setProcess(p)
                .build();

        CommunicationProtocol.Message suspectMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.EPFD_SUSPECT)
                .setEpfdSuspect(epfdSuspect)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(suspectMessage);
    }

    private void triggerRestore(CommunicationProtocol.ProcessId p) {
        CommunicationProtocol.EpfdRestore epfdRestore = CommunicationProtocol.EpfdRestore
                .newBuilder()
                .setProcess(p)
                .build();

        CommunicationProtocol.Message restoreMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.EPFD_RESTORE)
                .setEpfdRestore(epfdRestore)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(restoreMessage);
    }

    private void triggerPlSendHeartbeatRequest(CommunicationProtocol.ProcessId p) {
        CommunicationProtocol.EpfdInternalHeartbeatRequest epfdHeartbeatRequest = CommunicationProtocol.EpfdInternalHeartbeatRequest
                .newBuilder()
                .build();

        CommunicationProtocol.Message heartbeatRequestMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)
                .setEpfdInternalHeartbeatRequest(epfdHeartbeatRequest)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        CommunicationProtocol.PlSend plSend = CommunicationProtocol.PlSend
                .newBuilder()
                .setDestination(p)
                .setMessage(heartbeatRequestMessage)
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

    private void startTimer(int delay) {
        CommunicationProtocol.EpfdTimeout epfdTimeout = CommunicationProtocol.EpfdTimeout
                .newBuilder()
                .build();

        CommunicationProtocol.Message timeoutMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.EPFD_TIMEOUT)
                .setEpfdTimeout(epfdTimeout)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                process.addMessageToQueue(timeoutMessage);
            }
        }, delay);
    }
}
