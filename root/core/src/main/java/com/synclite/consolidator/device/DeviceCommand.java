package com.synclite.consolidator.device;

import com.synclite.consolidator.global.UserCommand;

public class DeviceCommand {
    public DeviceCommand(UserCommand type) {
        this.type = type;
    }
    public UserCommand type;

    @Override
    public String toString() {
        return type.toString();
    }
}
