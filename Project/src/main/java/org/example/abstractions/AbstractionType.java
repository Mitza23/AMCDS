package org.example.abstractions;

import lombok.Getter;

@Getter
public enum AbstractionType {
    PL("pl"),
    BEB("beb"),
    APP("app"),
    NNAR("nnar"),
    EPFD("epfd"),
    ELD("eld"),
    EP("ep"),
    EC("ec"),
    UC("uc");

    private final String id;

    AbstractionType(String id) {
        this.id = id;
    }
}
