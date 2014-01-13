#!/usr/bin/env python
import sys
import zmq
import time
import marshal
import pickle
import types
import optparse

INPUT_PORT = 9600
OUTPUT_PORT = 9601

class Slave(object):
    def __init__(self, address):
        self.context = zmq.Context()
        self.puller = self.context.socket(zmq.PULL)
        self.puller.connect("tcp://%s:%s" % (address, INPUT_PORT))
        self.pusher = self.context.socket(zmq.PUSH)
        self.pusher.connect("tcp://%s:%s" % (address, OUTPUT_PORT))

    def serve(self):
        while True:
            raw_task = self.puller.recv()
            task = marshal.loads(raw_task)
            fn = types.FunctionType(task['func_code'], globals(), "fn")
            output = self.process_function(fn, task['key'], task['val'])
            self.pusher.send(pickle.dumps(output))

    def process_function(self, fn, key, val):
        tmp = []
        for key, val in fn(key, val):
            tmp.append((key, val))
        return tmp


class Task(object):
    request_timeout = 100
    pull_retries = 3

    def __init__(self):
        self.context = zmq.Context()
        self.puller = self.context.socket(zmq.PULL)
        self.puller.bind("tcp://*:%s" % OUTPUT_PORT)
        self.pusher = self.context.socket(zmq.PUSH)
        self.pusher.bind("tcp://*:%s" % INPUT_PORT)
        self.poller = zmq.Poller()
        self.poller.register(self.puller, zmq.POLLIN)

    def empty_queue(self, output=[]):
        retries_left = self.pull_retries
        while retries_left:
            result = self.pull_result()
            if result is not None:
                output += result
            else:
                retries_left -= 1
        return output

    def pull_result(self):
        socks = dict(self.poller.poll(self.request_timeout))
        if self.puller in socks and socks[self.puller] == zmq.POLLIN:
            result = self.puller.recv()
            return pickle.loads(result)
        return None

    def map(self, input):
        return self.apply_function(enumerate(input), self.mapper)

    def combine(self, input):
        output = {}
        for entry in input:
            output.setdefault(entry[0], []).append(entry[1])
        return output

    def reduce(self, input):
        return self.apply_function(input.items(), self.reducer)

    def apply_function(self, iterable, function):
        # iterable should be a list of (key, value) pairs
        output = []
        for key, val in iterable:
            task = {"key": key, "val": val, "func_code": function.func_code}
            self.pusher.send(marshal.dumps(task))
            result = self.pull_result()
            if result is not None:
                output += result
        return self.empty_queue(output)

    def execute(self):
        output = self.map(self.input)
        output = self.combine(output)
        output = self.reduce(output)
        return output

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-a', '--address', dest='address', default='localhost',
                      help='IP address of master')
    (options, args) = parser.parse_args()
    Slave(options.address).serve()
