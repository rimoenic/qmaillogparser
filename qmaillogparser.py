#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import re
import datetime


class QmailLogParser(object):
    re_newmsg = re.compile(
        r'new msg (?P<message_id>[0-9]+)')
    re_infomsg = re.compile(
        r'info msg (?P<message_id>[0-9]+): bytes (?P<size>[0-9]*) from <(?P<from>[^>]*)>')
    re_startdeli = re.compile(
        r'starting delivery (?P<delivery_id>[0-9]+): msg (?P<message_id>[0-9]+) to (?P<direction>local|remote) (?P<to>.*)')
    re_deli = re.compile(
        r'(?P<unixtime>[0-9.]+) delivery (?P<delivery_id>[0-9]+): (?P<d_status>[^:]+): (?P<d_msg>.*)')
    re_endmsg = re.compile(
        r'(?P<unixtime>[0-9.]+) end msg (?P<message_id>[0-9]+)')

    def __init__(self, logfile, verbose=False):
        self.dict_messages = {}
        self.dict_deliveries = {}
        self.logfile = logfile
        self.verbose = verbose

    def parse(self):
        for line in self.logfile.readlines():
            if self._proc_newmsg_line(line):
                continue
            if self._proc_infomsg_line(line):
                continue
            if self._proc_startdeli_line(line):
                continue
            if self._proc_deli_line(line):
                continue
            if self._proc_endmsg_line(line):
                continue

    def debugprint(self):
        for key, val in self.dict_deliveries.items():
            print "d_id:" + str(key) + " ", val
        for key, val in self.dict_messages.items():
            print "m_id:" + str(key) + " ", val

    def print_msg_data(self, message_id):
        msg_data = self.dict_messages[message_id]
        if msg_data['direction'] == 'local':
            baseformat = "%(datetime)s %(to)s <== %(from)s "
        elif msg_data['direction'] == 'remote':
            baseformat = "%(datetime)s %(from)s ==> %(to)s "

        if self.verbose:
            str_format = baseformat + "(s: %(size)s, ds: %(d_status)s, dm: %(d_msg)s)"
        else:
            str_format = baseformat + "(ds: %(d_status)s)"
        print str_format % msg_data

    def _proc_generic(self, re_pattern, line, proc):
        m = re_pattern.search(line)
        if m is None:
            return False
        proc(m)
        return True

    def _proc_newmsg_line(self, line):
        def newmsg_proc(m):
            message_id = int(m.group('message_id'))
            if message_id in self.dict_messages:
                print 'Warning: Already exists the message_id. logfile is not good.'
                print self.dict_messages[message_id]
            # initialize
            self.dict_messages[message_id] = {}

        return self._proc_generic(QmailLogParser.re_newmsg, line, newmsg_proc)

    def _proc_infomsg_line(self, line):
        def infomsg_proc(m):
            tmp = dict(m.groupdict())
            message_id = int(tmp.pop('message_id'))
            if tmp['from'] == "":
                tmp['from'] = '(root)'
            try:
                if self.dict_messages[message_id] == {}:
                    self.dict_messages[message_id] = tmp
                else:
                    print 'Warning: Already exists the message_id data or invalid type. logfile is not good.'
                    print self.dict_messages[message_id]
            except KeyError:
                print 'Warning: This message_id(' + message_id + ') is not initialized. logfile is not good.'
                self.dict_messages[message_id] = tmp

        return self._proc_generic(QmailLogParser.re_infomsg, line, infomsg_proc)

    def _proc_startdeli_line(self, line):
        def startdeli_proc(m):
            tmp = dict(m.groupdict())
            message_id = int(tmp.pop('message_id'))
            delivery_id = int(tmp.pop('delivery_id'))
            if message_id not in self.dict_messages:
                # insert dummy
                self.dict_messages[message_id] = {'size': '?', 'from': "(Unknown)"}
            message_info = self.dict_messages[message_id]
            message_info.update(tmp)
            self.dict_deliveries[delivery_id] = message_id

        return self._proc_generic(QmailLogParser.re_startdeli, line, startdeli_proc)

    def _proc_deli_line(self, line):
        def deli_proc(m):
            tmp = dict(m.groupdict())
            delivery_id = int(tmp.pop('delivery_id'))
            unixtime = float(tmp.pop('unixtime'))
            if delivery_id in self.dict_deliveries:
                message_id = self.dict_deliveries[delivery_id]
                tmp.update({
                    'datetime': datetime.datetime.fromtimestamp(unixtime)
                })
                self.dict_messages[message_id].update(tmp)
                self.print_msg_data(message_id)
                self.dict_deliveries.pop(delivery_id)

        return self._proc_generic(QmailLogParser.re_deli, line, deli_proc)

    def _proc_endmsg_line(self, line):
        def endmsg_proc(m):
            message_id = int(m.group('message_id'))
            try:
                self.dict_messages.pop(message_id)
            except:
                print 'Warning: This message_id(' + message_id + ') had been already popped. logfile is not good.'

        return self._proc_generic(QmailLogParser.re_endmsg, line, endmsg_proc)


if __name__ == "__main__":
    qlp = QmailLogParser(sys.stdin, verbose=True)
    qlp.parse()
    #qlp.debugprint()
