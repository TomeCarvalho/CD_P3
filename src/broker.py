"""Message Broker"""
import enum
from typing import Dict, List, Any, Tuple
import selectors
import socket
import json
import pickle
import xml.etree.ElementTree as ET


class Serializer(enum.Enum):
    """Possible message serializers."""
    JSON = 0
    XML = 1
    PICKLE = 2

class Converter:
    def __init__(self, _format: Serializer):
        self.msg_format = _format

    def __repr__(self):
        formats = ["JSON", "XML", "PICKLE"]
        return formats[self.msg_format]
    
    def isInt(text: str) -> bool:
        try:
            return int(text) == text
        except:
            return False

    def serialize(self, msg: Dict):
        # converter chaves para str, já que o cringe xml n gosta >:(
        msg2 = {}

        for key,val in msg.items():
            msg2[str(key)] = str(val)

        if self.msg_format == Serializer.JSON:   # JSON
            ret = json.dumps(msg)
            ret = ret.encode(encoding='UTF-8', errors='replace')
        elif self.msg_format == Serializer.XML: # XML
            ret = ET.tostring(ET.Element("main", attrib=msg2))
        elif self.msg_format == Serializer.PICKLE: # PICKLE
            ret = pickle.dumps(msg)
        else:
            #print("Invalid message format.")
            ret = None

        length = len(ret).to_bytes(2, byteorder="big")
        return length + ret
    def deserialize(self, msg: bytes):
        if self.msg_format == Serializer.JSON:
            ret = json.loads(msg)
            return ret
        elif self.msg_format == Serializer.XML:
            dataBytes = ET.fromstring(msg)
            if dataBytes.tag == "main":
                return dataBytes.attrib
            return None
        elif self.msg_format == Serializer.PICKLE:
            return pickle.loads(msg)
        else:
            return None
    
    # self._msg_format
    @property
    def msg_format(self):
        return self._msg_format
    
    @msg_format.setter
    def msg_format(self, msg_format):
        self._msg_format = msg_format

    @msg_format.deleter
    def msg_format(self):
        del self._msg_format


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        self.topics = {}
        self.sock = socket.socket() # listening socket
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # reuse address
        self.sock.bind((self.host, self.port))
        self.sock.listen(100)
        self.sel = selectors.DefaultSelector() # selector
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept) # monitor with selector

    @staticmethod
    def list_topics_recursive(topics, ret = []) -> List[str]:
        for topic_name, topic_info in topics.items():
            if topic_info["value"] != None and topic_name not in ret:
                ret.append(topic_name)
            if topic_info["subtopics"] is not None:
                for subtopic_name, subtopic_info in topic_info["subtopics"].items():
                    Broker.list_topics_recursive({subtopic_name: subtopic_info}, ret)
        return ret

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics."""
        lst = Broker.list_topics_recursive(self.topics)
        #print("list_topics lst:", lst)
        return lst

    @staticmethod
    def merge_dicts(*dicts):
        ret = {}
        for dict in dicts:
            ret.update(dict)
        return ret

    def find_topic(self, topic: str):
        lst = [s for s in topic.split("/")]
        # "a" -> ['a']
        # "a/b" -> ['a', 'b'] -> ['a', 'a/b']
        # "/a" -> ['', 'a'] -> ['/', '/a']
        # "/a/b" -> ['', 'a', 'b'] -> ['/', '/a', '/a/b']
        if lst[0] == '':
            lst = ["/" + s for s in topic.split("/")]
        else:
            for i in range(1, len(lst)):
                lst[i] = "/" + lst[i]

        for i in range(1, len(lst)):
            if lst[i-1] != "/":
                lst[i] = lst[i - 1] + lst[i]

        topic = self.topics[lst[0]]
        for subtopic_name in lst[1:]:
            if subtopic_name not in topic["subtopics"]:
                topic["subtopics"][subtopic_name] = {"show": False, "value": None, "consumers": [], "subtopics": {}}

            topic = topic["subtopics"][subtopic_name]
        
        return topic

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        return self.find_topic(topic)["value"]
    
    def put_topic(self, topic, value):
        """Store in topic the value."""
        dic = self.find_topic(topic)
        dic["value"] = value
        dic["show"] = True

    def list_subscriptions(self, topic: str) -> List[socket.socket]:
        """Provide list of subscribers to a given topic."""
        return [consumer for consumer in self.find_topic(topic)["consumers"]]

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        #print("topic:",topic)
        lst = [s for s in topic.split("/")]

        if lst[0] == '':
            lst = ["/" + s for s in topic.split("/")]
        else:
            for i in range(1, len(lst)):
                lst[i] = "/" + lst[i]

        for i in range(1, len(lst)):
            if lst[i-1] != "/":
                lst[i] = lst[i - 1] + lst[i]
        
        if lst[0] not in self.topics:
            self.topics[lst[0]] = {"show": False, "value": None, "consumers": [], "subtopics": {}}
        topic = self.topics[lst[0]]
        for subtopic_name in lst[1:]:
            if subtopic_name not in topic["subtopics"]:
                topic["subtopics"][subtopic_name] = {"show": False, "value": None, "consumers": [], "subtopics": {}}
            #print(subtopic_name)
            topic = topic["subtopics"][subtopic_name]
            

        topic["show"] = True
        #print("lst:",lst)
        #print("True sus:",topic)
        topic["consumers"].append((address, _format))

        if topic["value"] is not None:
            send_msg = {"method": "SEND", "data": topic["value"]}
            conv = Converter(_format)
            byt = conv.serialize(send_msg)
            address.send(byt)

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        topic_consumers = self.find_topic(topic)["consumers"]
        for addr_ser in topic_consumers:
            if addr_ser[0] == address:
                topic_consumers.remove(addr_ser)
                address.close()
                return

    def run(self):
        """Run until canceled."""
        while not self.canceled:
            for key, _ in self.sel.select(): # self.sel.select(): events
                callback = key.data   # either accept or read (depends on connection)
                callback(key.fileobj) # accept or read the connection
    
    def accept(self, sock: socket.socket):
        conn, addr = sock.accept()
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)
    
    def read(self, conn: socket.socket):
        _format = conn.recv(1)
        if _format:
            _format = int.from_bytes(_format, byteorder="big")
            serializer = Serializer(_format)
            converter = Converter(serializer)
            length = int.from_bytes(conn.recv(2), byteorder="big")
            msg_bytes = conn.recv(length)
            msg = converter.deserialize(msg_bytes)
            method = msg["method"]
            if method == "SUBSCRIBE":
                self.subscribe(msg["topic"], conn, serializer)
            elif method == "PUBLICATE":
                self.publicate(msg)
            elif method == "UNSUBSCRIBE":
                self.unsubscribe(msg["topic"], conn)
            elif method == "REQ_TOPICS":
                topics = self.list_topics()
                dic = {"method": "REP_TOPICS", "lst":topics}
                conn.send(converter.serialize(dic))
            else:
                print("!!! CURSED MSG METHOD !!!")
        else:
            self.sel.unregister(conn)
            self.remove_consumer(conn, self.topics)
            conn.close()
    
    def remove_consumer(self, conn: socket.socket, topic: Dict):
        for topic_name, info in topic.items():
            for consumer in info["consumers"]:
                if consumer[0] == conn: # consumer[0]: address
                    info["consumers"].remove(consumer)
                    print("removed B)")
                    return
            self.remove_consumer(conn, info["subtopics"]) # só iá acontecer se existir pelo menos 1 tópico filho
        
    def publicate(self, msg: Dict):
        topic = msg["args"]["topic"]

        lst = [s for s in topic.split("/")]

        if lst[0] == '':
            lst = ["/" + s for s in topic.split("/")]
        else:
            for i in range(1, len(lst)):
                lst[i] = "/" + lst[i]

        for i in range(1, len(lst)):
            if lst[i-1] != "/":
                lst[i] = lst[i - 1] + lst[i]
        
        msg_to_send = {"method": "SEND", "data": msg["args"]["msg"]}
        msg_serialized = 3 * [None]

        if lst[0] not in self.topics:
            self.topics[lst[0]] = {"show": False, "value": None, "consumers": [], "subtopics": {}}
        topic = self.topics[lst[0]]
        for addr, s in topic["consumers"]:
            if msg_serialized[s.value] is None:
                conv = Converter(s)
                msg_serialized[s.value] = conv.serialize(msg_to_send)
            addr.send(msg_serialized[s.value])
        
        for subtopic_name in lst[1:]: # make our way into the desired topic
            if not subtopic_name in topic["subtopics"]:
                topic["subtopics"][subtopic_name] = {"show": False, "value": None, "consumers": [], "subtopics": {}}

            topic = topic["subtopics"][subtopic_name]
            for addr, s in topic["consumers"]:
                if msg_serialized[s.value] is None:
                    conv = Converter(s)
                    msg_serialized[s.value] = conv.serialize(msg_to_send)
                addr.send(msg_serialized[s.value])

        topic["value"] = msg["args"]["msg"]
    
    # self._host
    @property
    def host(self):
        return self._host
    
    @host.setter
    def host(self, host):
        self._host = host

    @host.deleter
    def host(self):
        del self._host

    # self._port
    @property
    def port(self):
        return self._port
    
    @port.setter
    def port(self, port):
        self._port = port

    @port.deleter
    def port(self):
        del self._port

    # self._topics
    @property
    def topics(self):
        return self._topics
    
    @topics.setter
    def topics(self, topics):
        self._topics = topics

    @topics.deleter
    def topics(self):
        del self._topics

    # self._sel
    @property
    def sel(self):
        return self._sel
    
    @sel.setter
    def sel(self, sel):
        self._sel = sel
    
    @sel.deleter
    def sel(self):
        del self._sel
    
    # self._sock
    @property
    def sock(self):
        return self._sock
    
    @sock.setter
    def sock(self, sock):
        self._sock = sock
    
    @sock.deleter
    def sock(self):
        del self._sock