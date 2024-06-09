package org.example.util;

import org.example.communication.CommunicationProtocol;

import java.util.Collection;
import java.util.Comparator;

public class ProcessUtil {
    public static CommunicationProtocol.ProcessId getMaxRankedProcess(Collection<CommunicationProtocol.ProcessId> processes) {
        return processes.stream()
                .max(Comparator.comparing(CommunicationProtocol.ProcessId::getRank))
                .orElse(null);
    }
}
