package com.geekbrains.geekmarketwinter.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

//@Component
public class CheckTask {
    private static final Logger logger = LoggerFactory.getLogger(CheckTask.class.getName());

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

//    http://www.cronmaker.com/
//    @Scheduled(cron = "0 0/1 * 1/1 * ?")
//    @Scheduled(fixedRate = 5000)
    public void reportCurrentTime() {
        logger.info("Текущее время {}", dateFormat.format(new Date()));
    }
}
