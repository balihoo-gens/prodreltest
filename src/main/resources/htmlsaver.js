/**
  * create a closure to capture log messages, result and call exit
  */
var prog = function() {
  var output = { messages: [], result: "" };
  return {
    log: function(s) {
      output.messages.push(s);
    },
    setRes: function(r) {
      output.result = r;
    },
    exit: function (ret) {
      console.log(JSON.stringify(output));
      phantom.exit(ret);
    }
  };
}();

function clip_page(page, clipSelector) {
  var clipRect = page.evaluate(function (selector) {
    var qs = document.querySelector(selector);
    if (qs) {
      return qs.getBoundingClientRect();
    } else {
      return null;
    }
  }, clipSelector);
  if (clipRect) {
    page.clipRect = {
        top:    clipRect.top,
        left:   clipRect.left,
        width:  clipRect.width,
        height: clipRect.height
    };
  }
}

function render_page(page, filename, quality) {
  var reason = "";
  try {
    options = {}
    console.log("quality" + quality)
    if (quality) {
      options.quality = quality
    }
    if (page.render(filename, options)) {
      prog.log("rendered page to " + filename);
      return true;
    }
  } catch (e) {
    reason = ": " + e.message;
  }
  prog.log("failed rendering to " + filename + reason);
  return false;
}

function save_page(page, filename, _) {
  var reason = "";
  try {
    var fs = require('fs');
    fs.write(filename, page.content, 'w');
    prog.log("saved page to " + filename);
    return true;
  } catch (e) {
    reason = ": " + e.message;
  }
  prog.log("failed saving to " + filename + reason);
  return false;
}

function get_page(url, data, filename, action, clipSelector, quality) {
  webpage = require("webpage");

  var ret = 0;

  page = webpage.create();
  page.settings.userAgent = "Balihoo Html Saver";
  function process_page(status) {
    if (status === "success") {
      prog.log("page " + url + " retrieved");
      if (clipSelector) clip_page(page, clipSelector);
      if (action(page, filename, quality)) {
        prog.setRes(filename);
      } else {
        ret = 2;
      }
    } else {
      prog.log("failed to get url " + url + " status: " + status);
      ret = 3;
    }
    page.close();
    prog.exit(ret);
  }

  if (data) {
    page.open(url, "post", data, process_page);
  } else {
    page.open(url, process_page);
  }
}

function main() {
  try {
    system = require("system");
    var input = JSON.parse(system.stdin.read());
    var action = (input.action === "render") ? render_page : save_page;
    get_page(
      input.source,
      input.data,
      input.target,
      action,
      input.clipselector,
      input.quality);
  } catch(e) {
    prog.log(" exception: " + e.message);
    prog.exit(4);
  }
}

main();
