#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Assignment 5 simulate one or more servers"""

from __future__ import division
import urllib2
import csv
import argparse


#URL = "http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv"

class Queue(object):
    """Queue functions to store items for server"""
    def __init__(self):
        self.items = []

    def is_empty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0, item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

class Server(object):
    """Sever fucntions to precess requests"""
    def __init__(self):
        self.current_task = None
        self.time_remaining = 0

    def tick(self):
        if self.current_task != None:
            self.time_remaining = self.time_remaining - 1
            if self.time_remaining <= 0:
                self.current_task = None

    def busy(self):
        if self.current_task != None:
            return True
        else:
            return False

    def start_next(self, new_task):
        self.current_task = new_task
        self.time_remaining = new_task.run_time

class Request(object):
    """Functions to handle server requests"""
    def __init__(self, enter_time, run_time):
        self.enter_time = enter_time
        self.run_time = run_time

    def get_wait_time(self):
        return self.enter_time

    def get_process_time(self):
        return self.run_time

    def wait_time(self, current_time):
        return current_time - self.enter_time

def simulate_one_server(input_url):
    """Simlulates a single server"""
    server = Server()
    server_queue = Queue()
    waiting_list = []
    time_counter = 0
    data = []
    first_request = False

    csv_data = csv.reader(urllib2.urlopen(input_url))
    for row in csv_data:
        data.append(row)

    while time_counter < int(data[0][0]):
        first_request = False
        time_counter += 1
    else:
        first_request = True

    if first_request:
        for item in data:
            enter_time = int(item[0])
            run_time = int(item[2])
            request = Request(enter_time, run_time)
            server_queue.enqueue(request)

            if (not server.busy()) and (not server_queue.is_empty()):
                next_request = server_queue.dequeue()
                waiting_list.append(next_request.wait_time(time_counter))
                server.start_next(request)
            time_counter += 1
            server.tick()

        average_wait = sum(waiting_list) / len(waiting_list)
        print "Average wait {:6.2f} secs {:3d}tasks remaining.".format(
            average_wait, server_queue.size())

def simulate_many_servers(input_url, server_quantity):
    """ Simulates multiple servers"""
    servers = []
    for item in range(0, server_quantity):
        servers.append(Server())

    server_queues = []
    for item in range(0, server_quantity):
        server_queues.append(Queue())

    wait_lists = []
    for item in range(0, server_quantity):
        wait_lists.append([])

    time_counter = 0
    server_count = 0
    data = []
    first_request = False

    csv_data = csv.reader(urllib2.urlopen(input_url))
    for row in csv_data:
        data.append(row)

    while time_counter < int(data[0][0]):
        first_request = False
        time_counter += 1
    else:
        first_request = True

    if first_request:
        for row in data:
            enter_time = int(row[0])
            run_time = int(row[2])
            request = Request(enter_time, run_time)
            server_queues[server_count].enqueue(request)
            if server_count < server_quantity - 1:
                server_count += 1
            else:
                server_count = 0

            if (not servers[server_count].busy()) and (not server_queues[server_count].is_empty()):
                next_request = server_queues[server_count].dequeue()
                wait_lists[server_count].append(next_request.wait_time(time_counter))
                servers[server_count].start_next(request)
            time_counter += 1
            servers[server_count].tick()

        for item in range(0, server_quantity):
            average_wait = sum(wait_lists[item]) / len(wait_lists[item])
            print "Average wait {:6.2f} secs {:3d} tasks remaining.".format(
                average_wait, server_queues[item].size())

def main():
    """Runs simulate function based on input"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', action="store")
    parser.add_argument('--servers', action="store")
    args = parser.parse_args()

    if args.servers:
        simulate_many_servers(args.url, int(args.servers))
    else:
        simulate_one_server(args.url)

if __name__ == '__main__':
    main()
