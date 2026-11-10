-- 压测用秒杀券：id=21，库存 100000（与 perf/seckill-bench.jmx 中的路径一致）
-- 幂等：重复执行只重置库存与有效期，不会新增记录
INSERT INTO tb_voucher (id, shop_id, title, sub_title, rules, pay_value, actual_value, type, status)
VALUES (21, 1, '秒杀压测券', '用于 200 / 500 / 1000 并发压测', '仅压测使用', 100, 1000, 1, 1)
ON DUPLICATE KEY UPDATE title = VALUES(title), status = 1;

INSERT INTO tb_seckill_voucher (voucher_id, stock, begin_time, end_time)
VALUES (21, 100000, NOW(), DATE_ADD(NOW(), INTERVAL 7 DAY))
ON DUPLICATE KEY UPDATE stock = VALUES(stock), begin_time = VALUES(begin_time), end_time = VALUES(end_time);
