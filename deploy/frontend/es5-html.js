// 批量把 html 内联 <script> 转成 ES5（兼容老浏览器内核）
const fs = require('fs');
const path = require('path');
const babel = require('@babel/core');

const dir = process.argv[2] || 'html';
const files = fs.readdirSync(dir).filter((f) => f.endsWith('.html'));

for (const f of files) {
  const file = path.join(dir, f);
  let html = fs.readFileSync(file, 'utf8');
  const re = /<script>([\s\S]*?)<\/script>/g;
  let changed = false;
  html = html.replace(re, (whole, code) => {
    if (!code.trim()) return whole; // 空脚本块跳过
    const out = babel.transformSync(code, {
      presets: [['@babel/preset-env', { targets: 'IE 11', modules: false }]],
    });
    changed = true;
    return '<script>' + out.code + '</script>';
  });
  if (changed) fs.writeFileSync(file, html, 'utf8');
  console.log(f + (changed ? ' [converted]' : ' [no inline script]'));
}
console.log('done');
