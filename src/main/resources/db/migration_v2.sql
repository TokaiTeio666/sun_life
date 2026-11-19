-- =====================================================================
-- migration_v2：为高频查询补充索引（可重复执行，已存在的索引自动跳过）
-- 已有数据卷的环境执行：
--   docker exec -i hmdp-mysql mysql -uroot -p123456 hmdp < src/main/resources/db/migration_v2.sql
-- 全新环境无需执行：hmdp.sql 已包含同样的索引
-- =====================================================================

DROP PROCEDURE IF EXISTS hmdp_add_index;
DELIMITER $$
CREATE PROCEDURE hmdp_add_index(IN p_table VARCHAR(64), IN p_index VARCHAR(64), IN p_ddl VARCHAR(512))
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.statistics
                   WHERE table_schema = DATABASE() AND table_name = p_table AND index_name = p_index) THEN
        SET @ddl = p_ddl;
        PREPARE stmt FROM @ddl;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END$$
DELIMITER ;

-- 订单：超时关单扫描 WHERE status=1 AND create_time<? ORDER BY create_time
CALL hmdp_add_index('tb_voucher_order', 'idx_status_ctime',
    'ALTER TABLE tb_voucher_order ADD INDEX idx_status_ctime (status, create_time)');
-- 订单：按用户/券查询订单、对账
-- 注意：不能建唯一索引——超时关单会把用户移出 Redis 去重集合，允许其再次抢购，同一 (user, voucher) 可能有多条订单
CALL hmdp_add_index('tb_voucher_order', 'idx_user_voucher',
    'ALTER TABLE tb_voucher_order ADD INDEX idx_user_voucher (user_id, voucher_id)');

-- 笔记：热门列表 ORDER BY liked DESC；个人主页 WHERE user_id=?
CALL hmdp_add_index('tb_blog', 'idx_liked',
    'ALTER TABLE tb_blog ADD INDEX idx_liked (liked)');
CALL hmdp_add_index('tb_blog', 'idx_user_id',
    'ALTER TABLE tb_blog ADD INDEX idx_user_id (user_id)');

-- 关注：关注关系唯一（防重复关注）；发笔记时 WHERE follow_user_id=? 查粉丝
CALL hmdp_add_index('tb_follow', 'uk_user_follow',
    'ALTER TABLE tb_follow ADD UNIQUE INDEX uk_user_follow (user_id, follow_user_id)');
CALL hmdp_add_index('tb_follow', 'idx_follow_user',
    'ALTER TABLE tb_follow ADD INDEX idx_follow_user (follow_user_id)');

-- 评论：按笔记查评论
CALL hmdp_add_index('tb_blog_comments', 'idx_blog_id',
    'ALTER TABLE tb_blog_comments ADD INDEX idx_blog_id (blog_id)');

-- 优惠券：按店铺查券（VoucherMapper.queryVoucherOfShop）
CALL hmdp_add_index('tb_voucher', 'idx_shop_id',
    'ALTER TABLE tb_voucher ADD INDEX idx_shop_id (shop_id)');

DROP PROCEDURE IF EXISTS hmdp_add_index;
