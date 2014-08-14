#call this as a package: python -m packager.test.splogtest
from ..splogger import Splogger

def splog(msg, log):
    with log.increased_indirection():
        log("UNKNOWN", "some message")
        log.debug("debug message")

log = Splogger(filename="test.log")
splog("test", log)
log("WARN", "things")

console_log = Splogger()
console_log.debug("console!")
console_log.debug("\"\\")

fail_log = Splogger(filename="/var/log/balihoo/test")
fail_log.exception("goes to /tmp/")


