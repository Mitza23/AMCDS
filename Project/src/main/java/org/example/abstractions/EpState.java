package org.example.abstractions;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.example.communication.CommunicationProtocol;

@Getter
@Setter
@AllArgsConstructor
public class EpState {
    private int valTimestamp;
    private CommunicationProtocol.Value val;
}
