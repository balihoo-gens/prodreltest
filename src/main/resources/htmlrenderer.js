system = require("system");
webpage = require("webpage");

var url = system.stdin.readLine();
page = webpage.create();
page.viewportSize = {
    width: 800,
    height: 600
};
page.settings.userAgent = "Balihoo Html Renderer";
page.open(url, function(status) {
    var filename = url.replace(/\W/g, "_") + '.png';
    console.log("saving to " + filename);
    if (status === "success") {
        page.render(filename);
    }
    page.close();
    return phantom.exit();
});

