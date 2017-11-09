# multi-paxos

![Build Status](https://travis-ci.org/roxel/multi-paxos.svg?branch=master)

Simple distributed key-value store using multi-paxos consensus protocol.

## Technical notes

* Originally implemented in Python 3.6

## Development

* virtualenvwrapper is recommended.

    mkvirtualenv -p python3.6 multi-paxos
    workon multi-paxos


## Core components

* Two types of processes can be started: clients and servers.
* All processes can be started using `node.py` file (see `python node.py --help`)
* All processes need a config file containing addresses to other consensus servers (see example: `config.yml`)
* Communication is done using TCP protocol. Server to server communication is asynchronous - servers do not respond to messages through the same socket stream, but send their own messages.
* Sending messages between clients and servers:
```
    from paxos.core import Node, Message
    node = Node(address='127.0.0.1:9999', node_id='99')
    message = Message(issuer_id='1', message_type=Message.MSG_READ)
    node.send_message(message)
```
* Servers automatically process messages based on their type. Messages are passed to `paxos.protocol.PaxosHandler` and appropriate handler methods are invoked, e.g. 'on_prepare', 'on_promise'.

## Storage

Servers store data in Redis. For local testing of the application they were configured to use the same Redis instance. 
Each server stores data in database with the same number as its id. 
Note that Redis limits may able (e.g. 16 database which can be changed in redis-server configs).