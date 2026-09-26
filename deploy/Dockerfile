# hm-dianping 应用镜像
# 构建上下文为项目根目录（docker-compose 中指定 context: ..）
# 基础镜像：Liberica JDK8（Debian），与 eclipse-temurin 同为上游 OpenJDK 发行版
FROM bellsoft/liberica-openjdk-debian:8

LABEL maintainer="hm-dianping" \
      description="hm-dianping backend (Spring Boot 2.7.18 / RocketMQ 4.9.7)"

WORKDIR /app

# 应用 jar（由 start 脚本先执行 mvn package 生成）
COPY target/hm-dianping-0.0.1-SNAPSHOT.jar /app/app.jar

EXPOSE 8081

# 时区对齐 Asia/Shanghai
ENV TZ=Asia/Shanghai

# 支持通过 JAVA_OPTS 注入 JVM 参数（云服务器低内存场景限制堆大小）
# exec 让 java 成为容器主进程，保证 SIGTERM 信号正确转发
ENV JAVA_OPTS=""
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
