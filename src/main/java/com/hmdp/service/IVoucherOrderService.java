package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    void createVoucherOrder(VoucherOrder voucherOrder);

    /**
     * 超时关单（延迟消息消费者调用）：订单 30 分钟未支付则自动取消并回补库存
     */
    void closeTimeoutOrder(VoucherOrder order);

    /**
     * 支付订单（测试/学习用：把未支付订单置为已支付）
     */
    Result payOrder(Long orderId);

}
