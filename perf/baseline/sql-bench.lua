-- sysbench 自定义脚本：并发重复执行环境变量 BENCH_SQL 中的一条业务 SQL
-- 由 component-bench.sh 调用，用于测「业务真实 SQL」在当前 MySQL 上的每秒执行次数与延迟
function thread_init()
  drv = sysbench.sql.driver()
  con = drv:connect()
  sql = os.getenv("BENCH_SQL")
end

function event()
  con:query(sql)
end

function thread_done()
  con:disconnect()
end
