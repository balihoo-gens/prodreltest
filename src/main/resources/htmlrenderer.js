try {
  system = require("system");
  webpage = require("webpage");

  //reads 1 line of stdin
  var input = JSON.parse(system.stdin.readLine());
  var url = input.source;
  var filename = "/tmp/htmlrenderer.png";
  var output = { messages: "", result: "" };
  var ret = 1;

  page = webpage.create();
  //page.viewportSize = { width: 800, height: 600 };
  page.settings.userAgent = "Balihoo Html Renderer";
  page.open(url, function(status) {
      if (status === "success") {
          output.messages += "page " + url + " retrieved\n";
          try {
            page.render(filename);
            output.messages += "saved to " + filename + "\n";
            output.result = filename;
            ret = 0;
          } catch (e) {
            output.messages += "failed rendering to " + filename + "\n";
          }
      } else {
        output.messages += "failed to get url " + url + "\n";
        output.messages += " status: " + status + "\n";
      }
      page.close();
      console.log(JSON.stringify(output));
      phantom.exit(ret);
  });
} catch(e) {
  console.log(JSON.stringify(output));
  phantom.exit(ret);
}
