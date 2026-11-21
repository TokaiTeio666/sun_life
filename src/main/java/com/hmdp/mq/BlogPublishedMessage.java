package com.hmdp.mq;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 笔记发布消息（topic {@code blog.published}，key = 作者 id）：消费者据此把笔记分发到粉丝的 Feed。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BlogPublishedMessage {
    private Long blogId;
    private Long userId;
    /** 发布时间（epoch 毫秒），作为 Feed 里的 score；收件箱和发件箱用同一个值，合并时才能去重 */
    private Long publishedAt;
}
