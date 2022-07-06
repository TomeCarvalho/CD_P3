"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from .broker import Serializer, Converter
import json
import pickle
import xml.etree.ElementTree as ET
import socket
from typing import Any


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        host = "localhost"
        port = 5000
        self.topic = topic
        self.sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sckt.connect((host,port))
        self.msg_format = None
        self.sub = {"method": "SUBSCRIBE", "topic":topic}
        self.converter = None
        self._type = _type

    def push(self, value):
        """Sends data to broker. """
        if self._type == MiddlewareType.PRODUCER:
            value = {"method":"PUBLICATE", "args":{"msg": value, "topic": self.topic}}
            #print("prod_send:",value)

        #print("value:",value)
        msg = self.converter.serialize(value)
        #print("msg:",msg)
        form = self.msg_format.to_bytes(1, byteorder="big")

        self.sckt.send(form+msg)

    def pull(self) -> (str, Any):
        """Waits for (topic, data) from broker.

        Should BLOCK the consumer!"""
        try:
            size = self.sckt.recv(2)
            length = int.from_bytes(size, "big")
            content = self.sckt.recv(length)

            dic = self.converter.deserialize(content)
            print("receive:",dic)
            method = dic["method"]
            if method == "SEND":
                return (self.topic, dic["data"])
            if method == "REP_TOPICS":
                return dic["lst"]

            return None
        except:
            dic = {"method": "UNSUBSCRIBE", "topic": self.topic}
            self.push(dic)
            quit()


    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        dic = {"method": "REQ_TOPICS"}
        self.push(dic)


    def cancel(self):
        """Cancel subscription."""
        dic = {"method": "UNSUBSCRIBE", "topic": self.topic}
        self.push(dic)
        quit()

        


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.msg_format = 0
        self.converter = Converter(Serializer(self.msg_format))
        #print(self.sub)
        if _type == MiddlewareType.CONSUMER:
            self.push(self.sub)

    # def push(self, value):
    #     form = 0
    #     data = json.dumps(value)
    #     content = {"form":form, "data":data}
    #     super().push(content)

    # def pull(self) -> (str, str):
    #     (topic, msg) = super().pull()

    #     newMsg = json.loads(msg)

    #     text = None
    #     if newMsg["method"] == "TOPIC_REP":
    #         text = newMsg["content"]

    #     return (topic, text)

    def cancel(self):
        super().cancel()
        self.push(self.canc)

        

class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.msg_format = 1
        self.converter = Converter(Serializer(self.msg_format))
        if _type == MiddlewareType.CONSUMER:
            self.push(self.sub)

    # def push(self, value):
    #     form = 1
    #     tree = ET.Element('main', attrib=value)
    #     super().push(ET.tostring(tree))

    # def pull(self, value) -> (str, str):
    #     (topic, msg) = super().pull()

    #     newMsg = ET.fromstring(msg)

    #     text = None

    #     if newMsg.tag == "main":
    #         dic = newMsg.attrib

    #         if dic["method"] == "TOPIC_REP":
    #             text = dic["content"]

    #     return (topic, text)

    def cancel(self):
        super().cancel()
        self.push(self.canc)


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.msg_format = 2
        self.converter = Converter(Serializer(self.msg_format))
        if _type == MiddlewareType.CONSUMER:
            self.push(self.sub)

    # def push(self, value):
    #     form = 2
    #     data = pickle.dumps(value)
    #     content = {"form":self.form, "data":data}
    #     super().push(content)


    # def pull(self) -> (str, str):
    #     (topic, msg) = super().pull()

    #     newMsg = pickle.loads(msg)

    #     text = None
    #     if newMsg["method"] == "TOPIC_REP":
    #         text = newMsg["content"]

    #     return (topic, text)

    def cancel(self):
        super().cancel()
        self.push(self.canc)
