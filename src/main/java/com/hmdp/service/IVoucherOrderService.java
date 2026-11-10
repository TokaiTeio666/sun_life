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
     * 扫描并关闭超时未支付订单（默认 30 分钟），回补 DB 与 Redis 库存
     *
     * @return 本次关闭的订单数
     */
    int closeTimeoutOrders();

    /**
     * 支付订单（测试/学习用：把未支付订单置为已支付）
     */
    Result payOrder(Long orderId);

}
