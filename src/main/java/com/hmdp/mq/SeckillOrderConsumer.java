package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.metrics.BizMetrics;
import com.hmdp.service.IVoucherOrderService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 秒杀订单 Kafka 批量消费者。
 *
 * <p>快速路径：一次 poll 的整批消息在一个事务里按券聚合扣库存 + 批量插入订单，
 * 热点库存行的加锁次数从「每条 1 次」降为「每批每券 1 次」。</p>
 *
 * <p>降级路径：快速路径失败（如脏消息、库存不一致）时逐条处理；某条仍失败则抛
 * {@link BatchListenerFailedException}，容器会提交它之前的位点，从这条开始按
 * DefaultErrorHandler 重试，耗尽后投递 {@code seckill.orders.DLT}。</p>
 *
 * <p>不再使用 Redisson 用户锁：userId 作为消息 key 保证同一用户落在同一分区串行消费；
 * 重复投递靠订单 id 主键幂等。原实现拿锁失败直接 ack，会丢掉已在 Redis 扣过库存的订单。</p>
 */
@Slf4j
@Component
public class SeckillOrderConsumer {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private BizMetrics bizMetrics;

    @KafkaListener(
            topics = KafkaConstants.SECKILL_ORDER_TOPIC,
            groupId = KafkaConstants.SECKILL_ORDER_GROUP,
            containerFactory = "seckillBatchListenerFactory",
            concurrency = "${hmdp.kafka.consumer.concurrency:3}"
    )
    public void onMessages(List<ConsumerRecord<String, String>> records) {
        if (records.isEmpty()) {
            return;
        }
        bizMetrics.consumeBatch(records.size());
        //1. 快速路径：整批解析 + 单事务批量落库
        try {
            List<VoucherOrder> orders = new ArrayList<>(records.size());
            for (ConsumerRecord<String, String> record : records) {
                orders.add(toOrder(record));
            }
            List<VoucherOrder> created = voucherOrderService.createVoucherOrdersBatch(orders);
            recordBatchCreated(created);
            bizMetrics.duplicatesConsumed(records.size() - created.size());
            voucherOrderService.cacheOrderStatus(created);
            log.debug("批量创建秒杀订单 received={} created={}", records.size(), created.size());
            return;
        } catch (Exception e) {
            bizMetrics.batchFallback();
            log.warn("批量创建秒杀订单失败，降级为逐条处理 size={} cause={}", records.size(), e.toString());
        }

        //2. 降级路径：逐条处理，定位失败消息
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, String> record = records.get(i);
            try {
                VoucherOrder order = toOrder(record);
                try {
                    if (voucherOrderService.createVoucherOrder(order)) {
                        bizMetrics.singleOrderCreated();
                        voucherOrderService.cacheOrderStatus(List.of(order));
                    }
                } catch (DuplicateKeyException dup) {
                    bizMetrics.duplicatesConsumed(1);
                    log.info("订单已存在，跳过重复消费 orderId={}", order.getId());
                }
            } catch (Exception e) {
                throw new BatchListenerFailedException("秒杀订单消费失败 offset=" + record.offset(), e, i);
            }
        }
    }

    /** 批量路径按券聚合：每张券一条库存 UPDATE */
    private void recordBatchCreated(List<VoucherOrder> created) {
        Map<Long, Long> byVoucher = created.stream()
                .collect(Collectors.groupingBy(VoucherOrder::getVoucherId, Collectors.counting()));
        byVoucher.values().forEach(n -> bizMetrics.batchOrdersCreated(n.intValue()));
    }

    private VoucherOrder toOrder(ConsumerRecord<String, String> record) {
        SeckillOrderMessage message = JSONUtil.toBean(record.value(), SeckillOrderMessage.class);
        if (message.getId() == null || message.getUserId() == null || message.getVoucherId() == null) {
            throw new IllegalArgumentException("非法秒杀订单消息: " + record.value());
        }
        return new VoucherOrder()
                .setId(message.getId())
                .setUserId(message.getUserId())
                .setVoucherId(message.getVoucherId())
                .setStatus(1);
    }
}
