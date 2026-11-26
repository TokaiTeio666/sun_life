# 知识笔记

开发与压测过程中整理的原理性知识。和 `docs/` 下其他文档的分工：

- `behavior-changes.md`、`capacity-baseline-report.md` 等记录**这个项目做了什么、测出了什么**；
- 这里记录**背后的原理和通用方法**，脱离本项目也能看懂，项目里的数字只作为例子。

| 文档 | 内容 |
| --- | --- |
| [connection-pool.md](connection-pool.md) | 连接池原理；MySQL（HikariCP）与 Redis（Lettuce）的区别；参数怎么定、怎么判断是否合适；`max-idle` 案例 |
| [capacity-concepts.md](capacity-concepts.md) | 拐点、安全容量、"只能当下限"的含义；如何区分压测端先到顶还是应用先到顶；不同机器结果不能横比 |
