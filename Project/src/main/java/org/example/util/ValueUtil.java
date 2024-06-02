package org.example.util;

import org.example.communication.CommunicationProtocol;

public class ValueUtil {
    public static CommunicationProtocol.Value buildUndefinedValue() {
        return CommunicationProtocol.Value
                .newBuilder()
                .setDefined(false)
                .build();
    }
}
