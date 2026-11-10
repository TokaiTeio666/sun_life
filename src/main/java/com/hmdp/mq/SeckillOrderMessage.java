package com.hmdp.mq;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 秒杀订单消息体
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillOrderMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 订单 id */
    private Long id;

    /** 用户 id */
    private Long userId;

    /** 优惠券 id */
    private Long voucherId;
}
