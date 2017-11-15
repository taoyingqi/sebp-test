package com.youzidata.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class LogTask {
    private final Logger Log = LoggerFactory.getLogger(this.getClass());

    @Scheduled(cron = "0/10 * * * * *")
    public void test() {
        Log.info("iiiiiiiiiiiiiiiiiiii");
        Log.debug("dddddddddddddddddd");
        try {
            int i = 10/0;
        } catch (Exception e) {
            Log.error(e.getMessage(), e);
        }
        Log.warn("wwwwwwwwwwwwwwwwwwww");
    }

}
