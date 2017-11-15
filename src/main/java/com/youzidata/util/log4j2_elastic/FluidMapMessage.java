package com.youzidata.util.log4j2_elastic;

import org.apache.logging.log4j.message.MapMessage;

public class FluidMapMessage
        extends MapMessage
{
    private static final long serialVersionUID = 3885431241176011273L;

    public FluidMapMessage add(String key, String value)
    {
        put(key, value);
        return this;
    }

    public FluidMapMessage message(String value)
    {
        put("message", value);
        return this;
    }

    public FluidMapMessage msg(String value)
    {
        return message(value);
    }
}
