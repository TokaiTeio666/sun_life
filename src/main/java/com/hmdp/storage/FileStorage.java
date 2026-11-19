package com.hmdp.storage;

import java.io.IOException;
import java.io.InputStream;

/**
 * 文件存储抽象。应用实例不再写本机磁盘，多实例部署时所有实例写同一个存储：
 * 默认 {@link LocalFileStorage} 写共享卷（docker volume / NFS），生产可替换为 OSS / MinIO / S3 实现。
 */
public interface FileStorage {

    /**
     * 保存文件
     *
     * @param path 相对路径，如 /blogs/3/7/uuid.jpg
     */
    void save(String path, InputStream in) throws IOException;

    /** 删除文件；文件不存在时静默返回 */
    void delete(String path) throws IOException;
}
