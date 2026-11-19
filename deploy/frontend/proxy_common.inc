# 公共反向代理配置（被 default.conf 中各 location include）
proxy_pass http://hmdp_app;
proxy_http_version 1.1;
# 清空 Connection 头，才能复用 upstream keepalive 长连接
proxy_set_header Connection "";
proxy_set_header Host $host;
proxy_set_header X-Real-IP $remote_addr;
proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
proxy_connect_timeout 2s;
proxy_send_timeout 10s;
proxy_read_timeout 10s;
# 某个副本连接失败/超时，自动切到下一个副本（仅对幂等请求重试）
proxy_next_upstream error timeout;
proxy_next_upstream_tries 2;
