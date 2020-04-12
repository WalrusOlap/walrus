package org.pcg.walrus.server;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("org.pcg.walrus.server.dao")
public class WalrusServer {

    public static void main(String[] args) {
        SpringApplication.run(WalrusServer.class, args);
    }

}
