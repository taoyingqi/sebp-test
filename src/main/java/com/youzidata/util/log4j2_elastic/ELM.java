package com.youzidata.util.log4j2_elastic;

public class ELM
{
    public static FluidMapMessage msg()
    {
        return new FluidMapMessage();
    }

    public static FluidMapMessage msg(String msg)
    {
        return new FluidMapMessage().add("message", msg);
    }
}

