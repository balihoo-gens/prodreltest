from splogger import Splogger

def splog(msg, log):
    log.inc_indirection()
    log("UNKNOWN", "some message")
    log.dec_indirection()

log = Splogger(filename="test.log")
splog("test", log)
log("DEBUG", "things")



