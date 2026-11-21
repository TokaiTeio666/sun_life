package com.hmdp.config;

import com.hmdp.dto.Result;
import com.hmdp.metrics.BizMetrics;
import com.hmdp.utils.SystemBusyException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.sql.SQLTransientConnectionException;

@Slf4j
@RestControllerAdvice
public class WebExceptionAdvice {

    private static final String BUSY_MESSAGE = "系统繁忙，请稍后再试";

    private final BizMetrics bizMetrics;

    public WebExceptionAdvice(BizMetrics bizMetrics) {
        this.bizMetrics = bizMetrics;
    }

    /** 并发闸主动拒绝：计数已在 DbGate 里做过，这里只负责映射响应。 */
    @ExceptionHandler(SystemBusyException.class)
    public ResponseEntity<Result> handleBusy(SystemBusyException e) {
        return busy();
    }

    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Result> handleRuntimeException(RuntimeException e) {
        if (isConnectionAcquireTimeout(e)) {
            // 等 DB 连接超时属于过载，不是程序错误：返回 503，不打堆栈（过载时每秒上千条，日志本身会拖慢应用）
            bizMetrics.overloadRejected(BizMetrics.OverloadReason.DB_CONN_TIMEOUT);
            log.debug("获取 DB 连接超时: {}", e.toString());
            return busy();
        }
        log.error(e.toString(), e);
        return ResponseEntity.ok(Result.fail("服务器异常"));
    }

    private static ResponseEntity<Result> busy() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(Result.fail(BUSY_MESSAGE));
    }

    /**
     * Hikari 等连接超时抛 SQLTransientConnectionException，外面会被 Spring / MyBatis 包上
     * CannotGetJdbcConnectionException、CannotCreateTransactionException、MyBatisSystemException 等，
     * 所以沿 cause 链查找。DB 宕机、认证失败是 SQLNonTransientConnectionException，不会命中，仍按异常处理。
     */
    private static boolean isConnectionAcquireTimeout(Throwable e) {
        for (int depth = 0; e != null && depth < 10; depth++, e = e.getCause()) {
            if (e instanceof SQLTransientConnectionException) {
                return true;
            }
        }
        return false;
    }
}
