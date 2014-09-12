var output = { messages: [], result: "" };

function clip_page(page, clipId) {
  var clipRect = page.evaluate(function (id) {
    var qs = document.querySelector(id);
    if (qs) {
      return qs.getBoundingClientRect();
    } else {
      return null;
    }
  }, clipId);
  if (clipRect) {
    page.clipRect = {
        top:    clipRect.top,
        left:   clipRect.left,
        width:  clipRect.width,
        height: clipRect.height
    };
  }
}

function render_page(url, filename, clipId, log) {
    webpage = require("webpage");

    var ret = 0;

    page = webpage.create();
    page.settings.userAgent = "Balihoo Html Renderer";
    page.open(url, function(status) {
      if (status === "success") {
        log.push("page " + url + " retrieved");
        try {
          if (clipId) {
            clip_page(page, clipId);
          }
          if (page.render(filename)) {
            log.push("saved to " + filename);
            output.result = filename;
          } else {
            log.push("failed rendering to " + filename);
          }
        } catch (e) {
          log.push("failed rendering to " + filename + ": " + e.message);
          ret = 2;
        }
      } else {
        log.push("failed to get url " + url);
        log.push(" status: " + status);
        ret = 3;
      }
      page.close();
      exit(ret);
    });
}

function exit (ret) {
  console.log(JSON.stringify(output));
  phantom.exit(ret);
}

function main() {
  try {
    system = require("system");
    //reads 1 line of stdin
    var input = JSON.parse(system.stdin.readLine());
    render_page(input.source, input.target, input.clipid, output.messages);
  } catch(e) {
    output.messages.push(" exception: " + e.message);
    exit(ret);
  }
}

main();
