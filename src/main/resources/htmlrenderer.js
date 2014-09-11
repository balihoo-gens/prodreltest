
var output = { messages: "", result: "" };
try {
  system = require("system");
  webpage = require("webpage");

  //reads 1 line of stdin
  var input = JSON.parse(system.stdin.readLine());
  var url = input.source;
  var filename = "/tmp/htmlrenderer.png";
  var ret = 0;

  page = webpage.create();
  page.settings.userAgent = "Balihoo Html Renderer";
  page.open(url, function(status) {
    if (status === "success") {
      output.messages += "page " + url + " retrieved\n";
      try {
        page.render(filename);
        output.messages += "saved to " + filename + "\n";
        output.result = filename;
      } catch (e) {
        output.messages += "failed rendering to " + filename + "\n";
        ret = 2;
      }
    } else {
      output.messages += "failed to get url " + url + "\n";
      output.messages += " status: " + status + "\n";
      ret = 3;
    }
      page.close();
      console.log(JSON.stringify(output));
      phantom.exit(ret);
    });
} catch(e) {
  ret = 4;
  output.messages += " exception: " + e.message + "\n";
  console.log(JSON.stringify(output));
  phantom.exit(ret);
}
