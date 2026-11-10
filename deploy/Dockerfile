# sun_life 应用镜像
# 构建上下文为项目根目录（docker-compose 中指定 context: ..）
# 基础镜像：Eclipse Temurin JDK17（Debian）
FROM eclipse-temurin:17-jre

LABEL maintainer="sun_life" \
      description="sun_life backend (Spring Boot 3.3.5 / Java 17 / Kafka 3.7)"

WORKDIR /app

# 应用 jar（由 start 脚本先执行 mvn package 生成）
COPY target/sun_life-0.0.1-SNAPSHOT.jar /app/app.jar

EXPOSE 8081

# 时区对齐 Asia/Shanghai
ENV TZ=Asia/Shanghai

# 支持通过 JAVA_OPTS 注入 JVM 参数（云服务器低内存场景限制堆大小）
# exec 让 java 成为容器主进程，保证 SIGTERM 信号正确转发
ENV JAVA_OPTS=""
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
