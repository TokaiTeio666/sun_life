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

    /** 逐条创建订单；主键冲突（重复消费）时抛 DuplicateKeyException；返回 false 表示 DB 库存不足 */
    boolean createVoucherOrder(VoucherOrder voucherOrder);

    /** 批量创建订单，返回实际新建的订单；任一步失败整体回滚并抛异常 */
    java.util.List<VoucherOrder> createVoucherOrdersBatch(java.util.List<VoucherOrder> orders);

    /** 写入订单状态短缓存（供前端轮询） */
    void cacheOrderStatus(java.util.Collection<VoucherOrder> orders);

    /** 查询订单状态：优先 Redis 短缓存；0 表示订单尚未创建 */
    Integer queryOrderStatus(Long orderId);

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
