from inspect import currentframe, getframeinfo,getouterframes
from collections import OrderedDict
import json
import datetime

class Splogger:
    def __init__(self, filename=None, system=None, component=None, additional_fields={}, indirection=0):
        #if you call 'log' directly, we're 2 frames removed from the call you want to log
        #  -> if the called wrapped the log file, they should add 1 to indirection to log the right call
        self._indirection = indirection + 2
        self._filename = filename
        #system and component
        self._system = system
        self._component = component
        self._additional_fields = additional_fields

    def __call__(self,level,msg):
        """ allows you to just call:
            log = Splogger(...)
            log("WARN", "stuff")
        """
        self.inc_indirection()
        self.log(level,msg)
        self.dec_indirection()

    def inc_indirection(self):
        self._indirection += 1

    def dec_indirection(self):
        self._indirection -= 1

    def caller_info(self):
        """ gets the stack up to the current frame
            and then digs down to the interesting call
            to return a tuple containing the file and line info
        """
        of = getouterframes(currentframe())
        if len(of) > self._indirection:
            return getframeinfo(of[self._indirection][0])
        return None

    def log(self, level, event, additional_fields={}):
        """ logs in splunk compliant format
        DEBUG level for application debugging
        INFO level for symantic logging
        WARN level for recoverable errors or automatic retry situations
        ERROR level for errors that are reported but not handled.
        EXCEPTION level for errors that are safely handled by the system
        """

        level = level.upper()
        if level not in ["DEBUG","INFO","WARN","ERROR","EXCEPTION"]:
            self.log("EXCEPTION", "unconventional log level %s:" % (level,))
        ci = self.caller_info()

        entry = OrderedDict()
        entry["time"] = str(datetime.datetime.now())
        entry["level"] = level.upper()
        entry["event"] = event
        entry["file"] = ci.filename if ci else "unknown"
        entry["line"] = ci.lineno if ci else "unknown"
        if self._system: entry["system"] = self._system
        if self._component: entry["component"] = self._component
        for (k,v) in self._additional_fields.iteritems():
            entry[k] = v
        for (k,v) in additional_fields.iteritems():
            entry[k] = v
        json_str_entry = "%s\n" % (json.dumps(entry),)
        if self._filename:
            with open(self._filename, "a") as f:
                f.write(json_str_entry)
        else:
            err = level in ["ERROR", "EXCEPTION"]
            f = sys.stderr if err else sys.stdout
            f.write(json_str_entry)

