# sun_life 前端（黑马点评静态页 + nginx 反向代理）
FROM nginx:1.18-alpine

# 前端静态资源（含 index.html / login.html 等）
COPY html/ /usr/share/nginx/html/

# 容器化 nginx 配置：静态页 + /api 反向代理到后端 app:8081
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
