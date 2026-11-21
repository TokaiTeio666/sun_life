package com.hmdp.utils;

/**
 * 过载保护主动拒绝请求时抛出，由 {@code WebExceptionAdvice} 统一映射为 HTTP 503「系统繁忙」。
 *
 * <p>过载时每秒可能抛出上万次，所以不采集堆栈（writableStackTrace = false）：
 * 堆栈对定位没有帮助，填充它的开销反而会加重过载。</p>
 */
public class SystemBusyException extends RuntimeException {

    public SystemBusyException(String message) {
        super(message, null, false, false);
    }
}
