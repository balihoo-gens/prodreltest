try {
  system = require("system");
  webpage = require("webpage");

  //reads 1 line of stdin
  var input = JSON.parse(system.stdin.readLine());
  var url = input.source;
  var filename = "/tmp/htmlrenderer.png";

  page = webpage.create();
  //page.viewportSize = { width: 800, height: 600 };
  page.settings.userAgent = "Balihoo Html Renderer";
  page.open(url, function(status) {
      if (status === "success") {
          console.log("saving to " + filename);
          page.render(filename);
      } else {
        console.log("failed to get url " + url)
        console.log(" status: " + status)
      }
      page.close();
      phantom.exit();
  });
} catch(e) {
  console.error(e.message)
  phantom.exit();
}
