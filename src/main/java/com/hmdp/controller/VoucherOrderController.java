package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @PostMapping("seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    /**
     * 查询订单状态（异步下单后，前端轮询此接口确认结果）
     * 返回：0=订单尚未创建（消息还在 RocketMQ 中排队/处理）；1=未支付；2=已支付；3=已核销；4=已取消；5=退款中；6=已退款
     */
    @GetMapping("status/{orderId}")
    public Result queryOrderStatus(@PathVariable("orderId") Long orderId) {
        VoucherOrder order = voucherOrderService.getById(orderId);
        if (order == null) {
            return Result.ok(0);
        }
        return Result.ok(order.getStatus());
    }

    /**
     * 支付订单（学习/测试用）：把"未支付"订单置为"已支付"
     * 用于验证延迟消息的两个分支：已支付的订单不会被超时关单
     */
    @PostMapping("pay/{orderId}")
    public Result payOrder(@PathVariable("orderId") Long orderId) {
        return voucherOrderService.payOrder(orderId);
    }
}
