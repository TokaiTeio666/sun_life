// let commonURL = "http://192.168.50.115:8081";
var commonURL = "/api";
// 设置后台服务地址
axios.defaults.baseURL = commonURL;
axios.defaults.timeout = 10000;
// request拦截器，将用户token放入头中
var token = sessionStorage.getItem("token");
axios.interceptors.request.use(function (config) {
  if (token) config.headers['authorization'] = token;
  return config;
}, function (error) {
  console.log(error);
  return Promise.reject(error);
});
axios.interceptors.response.use(function (response) {
  // 判断执行结果
  if (!response.data.success) {
    return Promise.reject(response.data.errorMsg);
  }
  return response.data;
}, function (error) {
  // 一般是服务端异常或者网络异常
  console.log(error);
  if (error.response && error.response.status == 401) {
    // 未登录，跳转
    setTimeout(function () {
      location.href = "/login.html";
    }, 200);
    return Promise.reject("请先登录");
  }
  var msg = "服务器异常";
  if (error.code === 'ECONNABORTED') {
    msg = "请求超时，请稍后重试";
  } else if (error.message) {
    msg = error.message;
  }
  return Promise.reject(msg);
});
axios.defaults.paramsSerializer = function (params) {
  var p = "";
  Object.keys(params).forEach(function (k) {
    if (params[k]) {
      p = p + "&" + k + "=" + params[k];
    }
  });
  return p;
};
var util = {
  commonURL: commonURL,
  getUrlParam: function getUrlParam(name) {
    var reg = new RegExp("(^|&)" + name + "=([^&]*)(&|$)", "i");
    var r = window.location.search.substr(1).match(reg);
    if (r != null) {
      return decodeURI(r[2]);
    }
    return "";
  },
  formatPrice: function formatPrice(val) {
    if (typeof val === 'string') {
      if (isNaN(val)) {
        return null;
      }
      // 价格转为整数
      var index = val.lastIndexOf(".");
      var p = "";
      if (index < 0) {
        // 无小数
        p = val + "00";
      } else if (index === p.length - 2) {
        // 1位小数
        p = val.replace("\.", "") + "0";
      } else {
        // 2位小数
        p = val.replace("\.", "");
      }
      return parseInt(p);
    } else if (typeof val === 'number') {
      if (!val) {
        return null;
      }
      var s = val + '';
      if (s.length === 0) {
        return "0.00";
      }
      if (s.length === 1) {
        return "0.0" + val;
      }
      if (s.length === 2) {
        return "0." + val;
      }
      var i = s.indexOf(".");
      if (i < 0) {
        return s.substring(0, s.length - 2) + "." + s.substring(s.length - 2);
      }
      var num = s.substring(0, i) + s.substring(i + 1);
      if (i === 1) {
        // 1位整数
        return "0.0" + num;
      }
      if (i === 2) {
        return "0." + num;
      }
      if (i > 2) {
        return num.substring(0, i - 2) + "." + num.substring(i - 2);
      }
    }
  }
};