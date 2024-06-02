package org.example.abstractions;


import lombok.Getter;
import org.example.communication.CommunicationProtocol;
import org.example.process.Process;

@Getter
public abstract class Abstraction {

    protected String abstractionId;
    protected Process process;

    protected Abstraction(String abstractionId, Process process) {
        this.abstractionId = abstractionId;
        this.process = process;
    }

    public abstract boolean handle(CommunicationProtocol.Message message);
}
