package com.hmdp.storage;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * 本地/共享目录存储。根目录由 hmdp.upload.dir 配置，Docker 部署时挂载为 app 与 nginx 共享的卷。
 * 所有路径都会规范化并校验必须位于根目录内，防止 ../ 路径穿越。
 */
@Component
public class LocalFileStorage implements FileStorage {

    private final Path root;

    public LocalFileStorage(@Value("${hmdp.upload.dir:./upload}") String dir) {
        this.root = Paths.get(dir).toAbsolutePath().normalize();
    }

    @Override
    public void save(String path, InputStream in) throws IOException {
        Path target = resolve(path);
        Files.createDirectories(target.getParent());
        Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void delete(String path) throws IOException {
        Files.deleteIfExists(resolve(path));
    }

    private Path resolve(String path) {
        String relative = path.startsWith("/") ? path.substring(1) : path;
        Path target = root.resolve(relative).normalize();
        if (!target.startsWith(root) || target.equals(root)) {
            throw new IllegalArgumentException("非法文件路径: " + path);
        }
        return target;
    }
}
