# 黑马点评（hm-dianping）后端接口文档

> 用于 Postman 接口配置，基于 
>
> `src/main/java/com/hmdp/controller/`
>
>  源码整理。
> 生成时间：2026-09-02



***

## 一、基础信息



| 项目       | 值                                                |
| -------- | ------------------------------------------------ |
| 服务名      | hmdp                                             |
| Base URL | `http://localhost:8081`                          |
| 端口       | `8081`（见 `application.yaml`）                     |
| 上下文路径    | 无                                                |
| 统一响应格式   | `Result`：\`{ "success": bool, "errorMsg": string |
| 鉴权方式     | 请求头 `authorization` 携带登录 token（Redis token）      |

### Postman 全局设置建议



* 新建 Collection，设置 Base URL 变量：`http://localhost:8081`

* 建环境变量 `token`，登录接口响应 `data` 里取 token 存入

* 需要登录的接口，Headers 添加：`authorization` = `{{token}}`

### 登录拦截规则（来自 MvcConfig）

以下路径**无需登录**（已被排除拦截），其余接口需在请求头携带 `authorization`：



```
/shop/\*\*        /voucher/\*\*      /shop-type/\*\*

/upload/\*\*      /blog/hot

/user/code      /user/login
```



***

## 二、接口总览（按模块）

### 1. 用户模块 `/user`



| 方法   | 路径                | 是否需登录 | 参数                                   | 说明                         |
| ---- | ----------------- | ----- | ------------------------------------ | -------------------------- |
| POST | `/user/code`      | 否     | form 参数 `phone`                      | 发送短信验证码（实际存 Redis，日志打印验证码） |
| POST | `/user/login`     | 否     | JSON Body `{phone, code, password?}` | 登录，`data` 返回 token         |
| POST | `/user/logout`    | 是     | 无                                    | ⚠️ 未实现，返回 "功能未完成"          |
| GET  | `/user/me`        | 是     | 无                                    | 获取当前登录用户信息                 |
| GET  | `/user/info/{id}` | 是     | 路径参数 `id`                            | 查询用户详情                     |

**登录流程示例：**



1. `POST /user/code`，Body 选择 `x-www-form-urlencoded`：



```
phone: 13800138000
```

（验证码在控制台日志里打印，或从 Redis `login:code:13800138000` 查看）



1. `POST /user/login`，Body 选择 `raw + JSON`：



```
{

&#x20; "phone": "13800138000",

&#x20; "code": "123456"

}
```



1. 响应 `data` 即为 token，存入环境变量 `token`：



```
{

&#x20; "success": true,

&#x20; "errorMsg": null,

&#x20; "data": "9d0d5c70-xxxx-xxxx-xxxx-xxxxxxxxxxxx",

&#x20; "total": null

}
```



***

### 2. 商铺模块 `/shop`



| 方法   | 路径              | 是否需登录 | 参数                              | 说明                  |
| ---- | --------------- | ----- | ------------------------------- | ------------------- |
| GET  | `/shop/{id}`    | 否     | 路径参数 `id`                       | 根据 id 查询商铺信息        |
| POST | `/shop`         | 否     | JSON Body `Shop`                | 新增商铺，`data` 返回商铺 id |
| PUT  | `/shop`         | 否     | JSON Body `Shop`（含 id）          | 更新商铺信息              |
| GET  | `/shop/of/type` | 否     | 查询参数 `typeId`、`current`(默认 1)   | 按商铺类型分页查询（每页 5 条）   |
| GET  | `/shop/of/name` | 否     | 查询参数 `name`(可选)、`current`(默认 1) | 按名称关键字分页查询（每页 10 条） |

**POST /shop 请求体示例：**



```
{

&#x20; "name": "测试店铺",

&#x20; "typeId": 1,

&#x20; "images": "/imgs/shop/1.png",

&#x20; "area": "陆家嘴",

&#x20; "address": "世纪大道100号",

&#x20; "x": 121.5,

&#x20; "y": 31.23,

&#x20; "avgPrice": 100,

&#x20; "sold": 0,

&#x20; "comments": 0,

&#x20; "score": 45,

&#x20; "openHours": "10:00-22:00"

}
```



***

### 3. 商铺类型模块 `/shop-type`



| 方法  | 路径                | 是否需登录 | 参数 | 说明       |
| --- | ----------------- | ----- | -- | -------- |
| GET | `/shop-type/list` | 否     | 无  | 查询商铺类型列表 |



***

### 4. 博客模块 `/blog`



| 方法   | 路径                | 是否需登录 | 参数                   | 说明                    |
| ---- | ----------------- | ----- | -------------------- | --------------------- |
| POST | `/blog`           | 是     | JSON Body `Blog`     | 发布探店笔记，`data` 返回博文 id |
| PUT  | `/blog/like/{id}` | 是     | 路径参数 `id`            | 点赞博文                  |
| GET  | `/blog/of/me`     | 是     | 查询参数 `current`(默认 1) | 查询我的博客（每页 10 条）       |
| GET  | `/blog/hot`       | 否     | 查询参数 `current`(默认 1) | 热门博客，按点赞数倒序           |



***

### 5. 优惠券模块 `/voucher`



| 方法   | 路径                       | 是否需登录 | 参数                         | 说明                    |
| ---- | ------------------------ | ----- | -------------------------- | --------------------- |
| POST | `/voucher`               | 否     | JSON Body `Voucher`        | 新增普通代金券，`data` 返回券 id |
| POST | `/voucher/seckill`       | 否     | JSON Body `Voucher`（含秒杀信息） | 新增秒杀券                 |
| GET  | `/voucher/list/{shopId}` | 否     | 路径参数 `shopId`              | 查询店铺优惠券列表             |

**POST /voucher/seckill 请求体示例**（秒杀券需带秒杀字段）：



```
{

&#x20; "shopId": 1,

&#x20; "title": "10元代金券",

&#x20; "subTitle": "全场通用",

&#x20; "rules": "单次消费满100可用",

&#x20; "payValue": 8000,

&#x20; "actualValue": 10000,

&#x20; "type": 1,

&#x20; "status": 1,

&#x20; "stock": 100,

&#x20; "beginTime": "2026-09-01T00:00:00",

&#x20; "endTime": "2026-09-30T23:59:59"

}
```

> 注：
>
> `stock`
>
> 、
>
> `beginTime`
>
> 、
>
> `endTime`
>
>  是秒杀券专用字段（非 
>
> `tb_voucher`
>
>  表字段，
>
> `@TableField(exist=false)`
>
> ），仅新增秒杀券时使用。



***

### 6. 秒杀订单模块 `/voucher-order`



| 方法   | 路径                            | 是否需登录 | 参数                | 说明                   |
| ---- | ----------------------------- | ----- | ----------------- | -------------------- |
| POST | `/voucher-order/seckill/{id}` | 是     | 路径参数 `id`（秒杀券 id） | 秒杀下单 ⚠️ 当前返回 "功能未完成" |



***

### 7. 图片上传模块 `/upload`



| 方法   | 路径                    | 是否需登录 | 参数                    | 说明                  |
| ---- | --------------------- | ----- | --------------------- | ------------------- |
| POST | `/upload/blog`        | 否     | multipart 文件字段 `file` | 上传博文图片，`data` 返回文件名 |
| GET  | `/upload/blog/delete` | 否     | 查询参数 `name`           | 删除已上传图片             |

**POST /upload/blog 请求方式：**



* Body 选择 `form-data`，key 为 `file`，类型选 File，value 选本地图片



***

## 三、空壳 Controller（暂无接口）



| Controller               | 前缀               | 状态      |
| ------------------------ | ---------------- | ------- |
| `FollowController`       | `/follow`        | 空，无任何接口 |
| `BlogCommentsController` | `/blog-comments` | 空，无任何接口 |



***

## 四、调试提示



1. **统一响应**：所有接口返回 `Result`，判断 `success` 是否为 `true`；失败时看 `errorMsg`。

2. **401 处理**：需要登录的接口未带 token 或 token 失效，返回 HTTP 401。

3. **验证码来源**：接口未真正发短信，验证码打印在控制台日志，Redis key 为 `login:code:{phone}`，2 分钟有效。

4. **token 有效期**：Redis key `login:token:{token}`，30 分钟（每次请求自动续期）。

5. **未实现接口**：`/user/logout`、`/voucher-order/seckill/{id}` 目前返回 `"功能未完成"`。