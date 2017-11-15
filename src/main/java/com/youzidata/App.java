package com.youzidata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Hello world!
 *
 */
@SpringBootApplication //equivalent @Configuration, @EnableAutoConfiguration and @ComponentScan
@EnableScheduling //启动定时任务
public class App {
    public static void main( String[] args ) {
        System.setProperty("elastic_local", "native://10.0.108.49:9300");
        System.setProperty("elastic_cluster", "es");
        System.setProperty("service", "sebp-test");
        System.setProperty("elastic_index", "sebp-test");
        SpringApplication.run(App.class, args);
    }
}
