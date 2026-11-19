package com.hmdp.controller;

import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.Result;
import com.hmdp.storage.FileStorage;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 笔记图片上传。文件写入 {@link FileStorage}（共享存储），任意实例上传、任意实例/nginx 都能读到。
 * 文件大小上限见 spring.servlet.multipart.max-file-size。
 */
@Slf4j
@RestController
@RequestMapping("upload")
public class UploadController {

    private static final Set<String> ALLOWED_SUFFIXES = Set.of("jpg", "jpeg", "png", "gif", "webp");

    /** 只允许删除本接口生成格式的文件名：/blogs/{0-15}/{0-15}/{uuid}.{ext} */
    private static final Pattern BLOG_IMAGE_NAME = Pattern.compile(
            "^/blogs/(1[0-5]|[0-9])/(1[0-5]|[0-9])/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}[.](jpg|jpeg|png|gif|webp)$");

    @Resource
    private FileStorage fileStorage;

    @PostMapping("blog")
    public Result uploadImage(@RequestParam("file") MultipartFile image) {
        String suffix = StrUtil.subAfter(image.getOriginalFilename(), ".", true).toLowerCase();
        if (!ALLOWED_SUFFIXES.contains(suffix)) {
            return Result.fail("仅支持 jpg/jpeg/png/gif/webp 图片");
        }
        String fileName = createNewFileName(suffix);
        try (InputStream in = image.getInputStream()) {
            fileStorage.save(fileName, in);
        } catch (IOException e) {
            throw new RuntimeException("文件上传失败", e);
        }
        log.debug("文件上传成功，{}", fileName);
        return Result.ok(fileName);
    }

    @GetMapping("/blog/delete")
    public Result deleteBlogImg(@RequestParam("name") String filename) {
        if (filename == null || !BLOG_IMAGE_NAME.matcher(filename).matches()) {
            return Result.fail("错误的文件名称");
        }
        try {
            fileStorage.delete(filename);
        } catch (IOException e) {
            throw new RuntimeException("文件删除失败", e);
        }
        return Result.ok();
    }

    private String createNewFileName(String suffix) {
        String name = UUID.randomUUID().toString();
        int hash = name.hashCode();
        int d1 = hash & 0xF;
        int d2 = (hash >> 4) & 0xF;
        return StrUtil.format("/blogs/{}/{}/{}.{}", d1, d2, name, suffix);
    }
}
