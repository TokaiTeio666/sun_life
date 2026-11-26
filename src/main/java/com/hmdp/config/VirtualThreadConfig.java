package com.hmdp.config;

import org.apache.tomcat.util.threads.VirtualThreadExecutor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.embedded.tomcat.TomcatProtocolHandlerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 虚拟线程（JDK 21）：只让 Tomcat 的请求处理线程使用虚拟线程。
 *
 * <p>每个请求一个虚拟线程，阻塞在 Redis / PostgreSQL 上时让出平台线程，并发不再受
 * {@code server.tomcat.threads.max} 限制，实际上限变成下游连接池（Hikari、Lettuce）。</p>
 *
 * <p><b>为什么不用 {@code spring.threads.virtual.enabled=true}</b>：那个开关会把 Kafka 监听容器和
 * {@code @Scheduled} 任务也放到虚拟线程上。JDK 21 中虚拟线程在 synchronized 块内阻塞时会「钉住」
 * 承载它的平台线程（平台线程数 = CPU 核数），而 Kafka 消费者在 poll() 里长时间阻塞正属于这种情况：
 * 实测 2 核容器里 3 个消费者就把平台线程占满，请求得不到调度，应用不再响应。
 * 所以消费者和定时任务保持原来的平台线程，这里只切换请求线程。</p>
 *
 * <p>同样的原因，PostgreSQL 驱动要用 42.6 及以上（内部的 synchronized 已换成锁，Spring Boot 3.3
 * 管理的版本是 42.7.x）；更早的版本每次查询都会钉住平台线程。</p>
 *
 * <p>回退 / A/B 对比：{@code VIRTUAL_THREADS=false}。</p>
 */
@Configuration
public class VirtualThreadConfig {

    @Bean
    @ConditionalOnProperty(name = "hmdp.web.virtual-threads", havingValue = "true")
    public TomcatProtocolHandlerCustomizer<?> tomcatVirtualThreadExecutor() {
        return protocolHandler -> protocolHandler.setExecutor(new VirtualThreadExecutor("tomcat-handler-"));
    }
}
