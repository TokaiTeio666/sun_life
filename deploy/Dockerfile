# hm-dianping 应用镜像
# 构建上下文为项目根目录（docker-compose 中指定 context: ..）
FROM eclipse-temurin:8-jre

LABEL maintainer="hm-dianping" \
      description="hm-dianping backend (Spring Boot 2.7.18 / RocketMQ 4.9.7)"

WORKDIR /app

# 应用 jar（由 start 脚本先执行 mvn package 生成）
COPY target/hm-dianping-0.0.1-SNAPSHOT.jar /app/app.jar

EXPOSE 8081

# 时区对齐 Asia/Shanghai
ENV TZ=Asia/Shanghai

ENTRYPOINT ["java", "-jar", "app.jar"]
