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
    
    def serialize(self, msg: Dict):
        if self.msg_format == Serializer.JSON:   # JSON
            ret = json.dumps(msg)
            ret = ret.encode(encoding='UTF-8', errors='replace')
        elif self.msg_format == Serializer.XML: # XML
            ret = ET.tostring(ET.Element("main", attrib=msg))
            #print("t:",type(ret))
        elif self.msg_format == Serializer.PICKLE: # PICKLE
            #ret = str(pickle.dumps(msg))
            ret = pickle.dumps(msg)
            #print("dict:",msg)
            #print("pickle str bytes:", ret)
            #teste = pickle.loads(ret)
            #teste = pickle.loads(ret.encode(encoding='UTF-8', errors='replace'))
            #print("teste:", teste)
        else:
            print("!!! INVALID msg_format !!!")
            ret = None
        #print("msg:",msg)
        #print("form:",self.msg_format)
        
        #print("ret:",ret)

        length = len(ret).to_bytes(2, byteorder="big")
        return length+ret
        #return length+ret.encode(encoding='UTF-8', errors='replace')

    def deserialize(self, msg: bytes):
        #print("des_msg:",msg)
        if self.msg_format == Serializer.JSON:
            ret = json.loads(msg)
            #print(ret)
            return ret
        elif self.msg_format == Serializer.XML:
            #print("str_xml_cursed:", msg)
            dataBytes = ET.fromstring(msg)
            if dataBytes.tag == "main":
                return dataBytes.attrib
            return None
        elif self.msg_format == Serializer.PICKLE:
            #print("cursed pickle:",msg)
            return pickle.loads(msg)
        else:
            #print("!!! INVALID msg_format !!!")
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
        # LOGGER.info("Listen @ %s:%s", self._host, self._port)
        self.topics = {"/": {"show": False, "value": None, "consumers": [], "subtopics": {}}}
        # topic: (name, show, value, subtopics, consumers)
        # socket and selector
        self.sock = socket.socket() # listening socket
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # reuse address
        self.sock.bind((self.host, self.port))
        self.sock.listen(100)
        self.sel = selectors.DefaultSelector() # selector
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept) # monitor with selector

    @staticmethod
    def list_topics_recursive(topics, ret = []) -> List[str]:
        # topic_name: "/somebody/once/told/me"
        # topic_info: {"subtopics": {"sub1": ... , "sub2": ...}, "consumers": [], "show": True}
        for topic_name, topic_info in topics.items():
            if topic_info["show"]:
                ret.append(topic_name)
            if topic_info["subtopics"] is not None:
                for subtopic_name, subtopic_info in topic_info["subtopics"].items():
                    Broker.list_topics_recursive({subtopic_name: subtopic_info}, ret)
        return ret

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics."""
        return Broker.list_topics_recursive(self.topics)

    @staticmethod
    def merge_dicts(*dicts):
        ret = {}
        for dict in dicts:
            ret.update(dict)
        return ret

    def find_topic(self, topic: str):
        lst = ["/" + s for s in topic.split("/")][1:]
        length = len(lst)
        # ['/somebody', '/once', '/told', '/me']

        if lst == ["/"]:
            return self.topics["/"]

        if length > 1:
            for i in range(1, length):
                lst[i] = lst[i - 1] + lst[i]
        # ['/somebody', '/somebody/once', '/somebody/once/told', '/somebody/once/told/me']

        topic = self.topics["/"]  # start at root topic
        for subtopic_name in lst: # make our way into the desired topic
            topic = topic["subtopics"][subtopic_name]
        
        return topic

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        return self.find_topic(topic)["value"]
    
    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.find_topic(topic)["value"] = value

    def list_subscriptions(self, topic: str) -> List[socket.socket]:
        """Provide list of subscribers to a given topic."""
        # stor disse no slack que eram apenas as subs explicitas
        return [consumer for consumer in self.find_topic(topic)["consumers"][0]]

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        print("broker.subscribe")
        lst = ["/" + s for s in topic.split("/")][1:]
        length = len(lst)
        # ['/somebody', '/once', '/told', '/me']
        topic = self.topics["/"]
        if lst == ["/"]:
            topic["consumers"].append((address, _format))
            topic["show"] = True
            return

        if length > 1:
            for i in range(1, length):
                lst[i] = lst[i - 1] + lst[i]
        # ['/somebody', '/somebody/once', '/somebody/once/told', '/somebody/once/told/me']

        # start at root topic
        for subtopic_name in lst: # make our way into the desired topic
            if subtopic_name not in topic["subtopics"]:
                topic["subtopics"][subtopic_name] = {"show": False, "value": None, "consumers": [], "subtopics": {}}

            topic = topic["subtopics"][subtopic_name]

        topic["show"] = True
        topic["consumers"].append((address, _format))
        if topic["value"] is not None:
            send_msg = {"method":"SEND", "data":topic["value"]}
            #print("before1:",send_msg)
            conv = Converter(_format)
            byt = conv.serialize(send_msg)
            #print("out1:",byt)
            address.sendall(byt)

        #print(self.topics)

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        topic_consumers = self.find_topic(topic)["consumers"]
        for addr_ser in topic_consumers:
            if addr_ser[0] == address:
                topic_consumers.remove(addr_ser)

        self.sel.unregister(address)
        address.close()
        #print(self.topics)

    def run(self):
        """Run until canceled."""
        while not self.canceled:
            for key, _ in self.sel.select(): # self.sel.select(): events
                callback = key.data   # either accept or read (depends on connection)
                callback(key.fileobj) # accept or read the connection
    
    # custom functions
    def accept(self, sock: socket.socket):
        print("DEBUG: broker accept()")
        conn, addr = sock.accept()
        print("DEBUG: broker accepted connection from", addr)
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)
    
    def read(self, conn: socket.socket):
        print("DEBUG: broker read()")
        _format = conn.recv(1)
        if _format:
            _format = int.from_bytes(_format, byteorder="big")
            serializer = Serializer(_format)
            converter = Converter(serializer)
            length = int.from_bytes(conn.recv(2), byteorder="big")
            msg_bytes = conn.recv(length)
            #print("msg:",msg_bytes)
            print("before deserialize:",msg_bytes)
            print("format:",_format)
            msg = converter.deserialize(msg_bytes)
            method = msg["method"]
            if method == "SUBSCRIBE":
                print("SUBSCRIBE method")
                #subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
                self.subscribe(msg["topic"], conn, serializer)
            elif method == "PUBLICATE":
                print("PUBLICATE method")
                self.publicate(msg)
            elif method == "UNSUBSCRIBE":
                print("UNSUBSCRIBE method")
                self.unsubscribe(msg["topic"], conn)
            else:
                print("!!! CURSED MSG METHOD !!!")
        else:
            print("F in the chat")
            self.sel.unregister(conn)
            self.remove_consumer(conn, self.topics["/"])
            conn.close()
    
    def remove_consumer(self, conn: socket.socket, topic: Dict):
        for consumer in topic["consumers"]:
            if consumer[0] == conn: # consumer[0]: address
                topic["consumers"].remove(consumer)
                return
        for subtopic_name in topic["subtopics"]:
            self.remove_consumer(conn, topic["subtopics"][subtopic_name])
        
    def publicate(self, msg: Dict):
        print("broker.publicate")
        topic = msg["args"]["topic"]
        lst = ["/" + s for s in topic.split("/")][1:]
        length = len(lst)
        # ['/somebody', '/once', '/told', '/me']

        if lst == ["/"]:
            lst = []

        if length > 1:
            for i in range(1, length):
                lst[i] = lst[i - 1] + lst[i]
        # ['/somebody', '/somebody/once', '/somebody/once/told', '/somebody/once/told/me']

        msg_to_send = {"method": "SEND", "data": msg["args"]["msg"]}
        msg_serialized = 3 * [None]

        topic = self.topics["/"]  # start at root topic
 
        for (addr, s) in topic["consumers"]:
            if msg_serialized[s.value] is None:
                print("before2:",msg_to_send)
                conv = Converter(s)
                msg_serialized[s.value] = conv.serialize(msg_to_send)
            addr.sendall(msg_serialized[s.value])

        for subtopic_name in lst: # make our way into the desired topic
            if not subtopic_name in topic["subtopics"]:
                topic["subtopics"][subtopic_name] = {"show": False, "value": None, "consumers": [], "subtopics": {}}

            topic = topic["subtopics"][subtopic_name]
            for (addr, s) in topic["consumers"]:
                if msg_serialized[s.value] is None:
                    print("before3:",msg_to_send)
                    conv = Converter(s)
                    msg_serialized[s.value] = conv.serialize(msg_to_send)
                addr.sendall(msg_serialized[s.value])

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