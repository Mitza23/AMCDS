package org.example.process;

import lombok.Getter;
import org.example.abstractions.Abstraction;
import org.example.abstractions.AbstractionType;
import org.example.abstractions.Application;
import org.example.abstractions.NNAtomicRegister;
import org.example.communication.CommunicationProtocol;
import org.example.communication.MessageReceiver;
import org.example.communication.MessageSender;
import org.example.util.AbstractionIdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
public class Process implements Runnable, Observer {

    private static final Logger log = LoggerFactory.getLogger(Process.class);
    private final CommunicationProtocol.ProcessId hub;
    private final BlockingQueue<CommunicationProtocol.Message> messageQueue;
    private final Map<String, Abstraction> abstractions;
    private CommunicationProtocol.ProcessId process;
    private List<CommunicationProtocol.ProcessId> processes;
    private String systemId;

    public Process(CommunicationProtocol.ProcessId process, CommunicationProtocol.ProcessId hub) throws InterruptedException {
        this.process = process;
        this.hub = hub;
        this.messageQueue = new LinkedBlockingQueue<>();
        this.abstractions = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        log.info("Running process {}-{}", process.getOwner(), process.getIndex());

        //register abstractions
        registerAbstraction(new Application(AbstractionType.APP.getId(), this));

        // start event loop
        Runnable eventLoop = () -> {
            while (true) {
                try {
                    CommunicationProtocol.Message message = messageQueue.take();
                    log.info("Handling {}; FromAbstractionId: {}; ToAbstractionId: {}", message.getType(), message.getFromAbstractionId(), message.getToAbstractionId());
                    if (!abstractions.containsKey(message.getToAbstractionId())) {
                        if (message.getToAbstractionId().contains(AbstractionType.NNAR.getId())) {
                            registerAbstraction(new NNAtomicRegister(AbstractionIdUtil.getNamedAncestorAbstractionId(message.getToAbstractionId()), this));
                        }
                    }
                    if (abstractions.containsKey(message.getToAbstractionId())) {
                        if (!abstractions.get(message.getToAbstractionId()).handle(message) && requeueMessage(message)) {
                            addMessageToQueue(message);
                        }
                    } else {
                        addMessageToQueue(message);
                    }
                } catch (InterruptedException interruptedException) {
                    log.error("Error handling message.");
                }
            }
        };
        String processName = String.format("%s-%d : %d", process.getOwner(), process.getIndex(), process.getPort());
        Thread eventLoopThread = new Thread(eventLoop, processName);
        eventLoopThread.start();

        MessageReceiver messageReceiver = new MessageReceiver(process.getPort());
        messageReceiver.addObserver(this);
        Thread messageReceiverThread = new Thread(messageReceiver, processName);
        messageReceiverThread.start();

        register();

        try {
            messageReceiverThread.join();
            eventLoopThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void registerAbstraction(Abstraction abstraction) {
        abstractions.putIfAbsent(abstraction.getAbstractionId(), abstraction);
    }

    public void addMessageToQueue(CommunicationProtocol.Message message) {
        try {
            messageQueue.put(message);
        } catch (InterruptedException e) {
            log.error("Error adding message to queue.");
        }
    }

    private boolean requeueMessage(CommunicationProtocol.Message message) {
        return CommunicationProtocol.Message.Type.PL_DELIVER.equals(message.getType()) &&
                (CommunicationProtocol.Message.Type.NNAR_INTERNAL_VALUE.equals(message.getPlDeliver().getMessage().getType()) ||
                        CommunicationProtocol.Message.Type.NNAR_INTERNAL_ACK.equals(message.getPlDeliver().getMessage().getType()));
    }


    // Send Message(NetworkMessage(Message(ProcRegistrationMessage)))
    private void register() {
        CommunicationProtocol.ProcRegistration procRegistration = CommunicationProtocol.ProcRegistration
                .newBuilder()
                .setOwner(process.getOwner())
                .setIndex(process.getIndex())
                .build();

        CommunicationProtocol.Message procRegistrationMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.PROC_REGISTRATION)
                .setProcRegistration(procRegistration)
                .setMessageUuid(UUID.randomUUID().toString())
                .setToAbstractionId(AbstractionType.APP.getId())
                .build();

        CommunicationProtocol.NetworkMessage networkMessage = CommunicationProtocol.NetworkMessage
                .newBuilder()
                .setSenderHost(process.getHost())
                .setSenderListeningPort(process.getPort())
                .setMessage(procRegistrationMessage)
                .build();

        CommunicationProtocol.Message outerMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(networkMessage)
                .setToAbstractionId(procRegistrationMessage.getToAbstractionId())
                .setMessageUuid(UUID.randomUUID().toString())
                .build();

        MessageSender.send(outerMessage, hub.getHost(), hub.getPort());
    }

    /**
     * This method handles an incoming Message.
     * If message is of type PROC_INITIALIZE_SYSTEM, it is handled by the process.
     * Otherwise, the message is added to the queue.
     * Triggered by calling the notifyObservers() method from the MessageReceiver.
     *
     * @param o   source of the message
     * @param arg incoming message
     */
    @Override
    public void update(Observable o, Object arg) {
        log.debug("Received message");
        if (arg instanceof CommunicationProtocol.Message) {
            CommunicationProtocol.Message message = (CommunicationProtocol.Message) arg;
            CommunicationProtocol.Message innerMessage = message.getNetworkMessage().getMessage();
            if (CommunicationProtocol.Message.Type.PROC_INITIALIZE_SYSTEM.equals(innerMessage.getType())) { // initialize system
                handleProcInitializeSystem(innerMessage);
            } else { // add message to queue as it can be and will be handled by the event loop
                messageQueue.add(message);
            }
        }
    }

    private void handleProcInitializeSystem(CommunicationProtocol.Message message) {
        log.debug("Handling PROC_INITIALIZE_SYSTEM");
        CommunicationProtocol.ProcInitializeSystem procInitializeSystem = message.getProcInitializeSystem();
        this.processes = procInitializeSystem.getProcessesList();
        this.process = getProcessByHostAndPort(this.process.getHost(), this.process.getPort()).get();
        this.systemId = message.getSystemId();
    }

    public Optional<CommunicationProtocol.ProcessId> getProcessByHostAndPort(String host, int port) {
        return processes.stream()
                .filter(p -> host.equals(p.getHost()) && p.getPort() == port)
                .findFirst();
    }
}
