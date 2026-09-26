Vue.component("footBar", {
  template: "\n    <div class=\"foot\">\n    <div class=\"foot-box\" :class=\"{active: activeBtn === 1}\" @click=\"toPage(1)\">\n      <div class=\"foot-view\"><i class=\"el-icon-s-home\"></i></div>\n      <div class=\"foot-text\">\u9996\u9875</div>\n    </div>\n    <div class=\"foot-box\" :class=\"{active: activeBtn === 2}\" @click=\"toPage(2)\">\n      <div class=\"foot-view\"><i class=\"el-icon-map-location\"></i></div>\n      <div class=\"foot-text\">\u5730\u56FE</div>\n    </div>\n    <div class=\"foot-box\" @click=\"toPage(0)\">\n      <img class=\"add-btn\" src=\"/imgs/add.png\" alt=\"\">\n    </div>\n    <div class=\"foot-box\" :class=\"{active: activeBtn === 3}\" @click=\"toPage(3)\">\n      <div class=\"foot-view\"><i class=\"el-icon-chat-dot-round\"></i></div>\n      <div class=\"foot-text\">\u6D88\u606F</div>\n    </div>\n    <div class=\"foot-box\" :class=\"{active: activeBtn === 4}\" @click=\"toPage(4)\">\n      <div class=\"foot-view\"><i class=\"el-icon-user\"></i></div>\n      <div class=\"foot-text\">\u6211\u7684</div>\n    </div>\n  </div>\n  ",
  data: function data() {
    return {};
  },
  props: ['activeBtn'],
  methods: {
    toPage: function toPage(i) {
      if (i === 0) {
        location.href = "/blog-edit.html";
      } else if (i === 4) {
        location.href = "/info.html";
      } else if (i === 1) {
        location.href = "/";
      }
    }
  }
});