package org.example.abstractions;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.example.communication.CommunicationProtocol;
import org.example.util.ValueUtil;

@Getter
@Setter
@AllArgsConstructor
public class NNARValue {

    private int timestamp;
    private int writerRank;
    private CommunicationProtocol.Value value;

    public NNARValue() {
        timestamp = 0;
        writerRank = 0;
        value = ValueUtil.buildUndefinedValue();
    }
}