package com.hmdp;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.TimeZone;

@MapperScan("com.hmdp.mapper")
@SpringBootApplication
@EnableAspectJAutoProxy(exposeProxy = true)
@EnableScheduling
public class HmDianPingApplication {

    public static void main(String[] args) {
        // 固定东八区，不依赖宿主机 / 容器的 TZ：pgjdbc 把 JVM 默认时区作为会话 TimeZone 发给 PG，
        // 库里 DEFAULT CURRENT_TIMESTAMP 和 update_time 触发器按会话时区取时间；
        // 应用里的 LocalDateTime.now()（如超时关单的截止时间）也必须与之一致，否则相差 8 小时
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        SpringApplication.run(HmDianPingApplication.class, args);
    }

}
