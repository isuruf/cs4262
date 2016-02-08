import socket
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from ttypes import Node
from dist import Client, Processor
from bootstrap_server import BootstrapServerConnection
import threading
from uuid import uuid1
from collections import deque

try:
    import thread
except ImportError:
    import _thread as thread

def send_search(users, filename, me):
    '''
    Send a search query to list of users
    Args:
        users (list(Node))       : list of users to send the search query
        filename (str)   		 : file name to search for
        me (Node)         		 : Search initiater Node
    '''
    uuid = str(uuid1())
    for user in users:
        with DistributedClient(user) as c:
            c.search(filename, me, 3, uuid)

class DistributedClient:
	'''
	Wrapper for RPC Client
	'''
    def __init__(self, user):
        self.user = user
        self.connected = None

    def __enter__(self):
        try:
            # Make socket
            self.transport = TSocket.TSocket(self.user.ip, self.user.port)

            # Buffering is critical. Raw sockets are very slow
            self.transport = TTransport.TBufferedTransport(self.transport)

            # Wrap in a protocol
            protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

            # Create a client to use the protocol encoder
            self.client = Client(protocol)

            # Connect!
            self.transport.open()

        except Thrift.TException as tx:
            print(('%s' % (tx.message)))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.transport.close()

    def join(self, me):
		'''
		Join node to the system
		Args:
			me(Node)	: Node to be connected
		'''
        try:
            print("Sending join request to %s " % self.user)
            self.client.join(me)
        except Thrift.TException as tx:
            pass#print(('Joining %s:%s failed with exception %s' % (self.user.port, self.user.ip, tx.message)))

    def leave(self, me):
		'''
		Remove node from the system
		Args:
			me(Node)	: Node to be removed
		'''
        try:
            self.client.leave(me)
        except:
            pass#print(('Leaving %s:%s failed with exception %s' % (self.user.port, self.user.ip, tx.message)))

    def search(self, filename, requester, hops, uuid):
		'''
		Search file 
		Args:
			filename(str)	: File name to search for
			requester(Node)	: Search requester node
			hops(int)		: Maximum number of hops
			uuid(str)		: uuid for the message
		'''
        try:
            self.client.search(filename, requester, hops, uuid)
        except Thrift.TException as tx:
            pass#print(('Sending filename(%s) search to %s:%s failed with exception %s' % (filename, self.user.port, self.user.ip, tx.message)))

    def found_file(self, files, requester, uuid):
		'''
		Search file 
		Args:
			files(list(str)): Matching set of files 
			requester(Node)	: Search requester node
			uuid(str)		: uuid for the message
		'''
        try:
            self.client.found_file(files, requester, uuid)
        except Thrift.TException as tx:
            pass#print(('Sending files(%s) found to %s:%s failed with exception %s' % (files, self.user.port, self.user.ip, tx.message)))


class DistributedServer:
    def __init__(self, users, me, files):
        self.users = users
        self.me = me
        self.files = files
        print("%s has the files %s" % (self.me, self.files))

        processor = Processor(self)
        transport = TSocket.TServerSocket(port=self.me.port)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        self.server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
        self.received_searches = deque([], 10)

    # rpc method
    def join(self, other):
		'''
		Join with distributed system using RPC
		Args:
			other(Node)		: Connecting node
		Returns:
			0   			: If node joined successfully
			9999   			: If node is already in the user list
		'''
        print("Received join request from %s" % other)
        if other in self.users:
            return 9999
        else:
            self.users.append(other)
            print("Routing table size %s. Entries %s" % (len(self.users), self.users))
            return 0

    # rpc method
    def leave(self, other):
		'''
		Leave distributed system using RPC
		Args:
			other(Node)		: Leaving node
		Returns:
			0   			: If node removed successfully
			9999   			: If node is not in the user list
		'''
        if other in self.users:
            self.users.remove(other)
            return 0
        else:
            return 9999

    # rpc method
    def found_file(self, files, node, uuid):
		'''
		
		Args:
			files(list(str))	: Matching set of files 
			node(Node)			: Node which has the file
			uuid(str)			: uuid for the message
		'''
        print("Found files %s from %s" % (files, node))

    # rpc method
    def search(self, filename, requester, hops, uuid):
		'''
		Search files within distributed system using RPC
		Args:
			filename(str)		: File name to search for
			requester(Node)		: Search requester node
			hops(int)			: Maximum number of hops
			uuid(str)			: uuid for the message
		'''
        if uuid in self.received_searches:  
            print("Duplicate search request %s" % uuid)
            return
        self.received_searches.append(uuid)
        #print("messages %s" % self.received_searches)
        print("Received search request %s from %s for filename %s" % (uuid, requester, filename))
        if requester != self.me and requester not in self.users:
            self.users.append(requester)
            print("Routing table size %s. Entries %s" % (len(self.users), self.users))
        l = []
        filename = " %s " % filename.strip().lower()
        for f in self.files:
            if filename in (" %s "%f.lower()):
                l.append(f)
        if len(l) != 0:
            threading.Thread(target=self.send_found, args=(l, requester, uuid)).start()
        elif hops > 1:
            threading.Thread(target=self.forward_request, args=(filename, requester, hops-1, uuid)).start()

    def forward_request(self, filename, requester, hops, uuid):
		'''
		Forward search request
		Args:
			filename(str)		: File name to search for
			requester(Node)		: Search requester node
			hops(int)			: Maximum number of hops
			uuid(str)			: uuid for the message
		'''
        print("Forwarding request to %s" % self.users)
        for user in self.users:
            if user == requester:
                continue
            with DistributedClient(user) as c:
                c.search(filename, requester, hops, uuid)

    def send_found(self, files, requester, uuid):
		'''
		Send file to the destination
		Args:
			files(list(str))	: Matching set of files 
			requester(Node)		: Search requester node
			hops(int)			: Maximum number of hops
			uuid(str)			: uuid for the message
		'''
        print("Sending file %s found to %s" % (files, requester))
        with DistributedClient(requester) as c:
            c.found_file(files, self.me, uuid)

    def __enter__(self):
        return self

    def serve(self):
        for user in self.users:
            with DistributedClient(user) as c:
                c.join(self.me)
        self.server.serve()

    def __exit__(self, exc_type, exc_value, traceback):
        for user in self.users:
            with DistributedClient(user) as c:
                c.leave(self.me)

import argparse
import textwrap

parser = argparse.ArgumentParser(
        prog='Distributed',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
           Example of use:
           python distributed.py 192.168.123.104:1089 vipula 192.168.123.103:1103 files.txt
           '''))

parser.add_argument("addrme", help="IP port pair of this node. eg: 192.168.123.104:1089")
parser.add_argument("name", help="Name of this node")
parser.add_argument("addrbs", help="IP port pair of bootstrap server eg: 192.168.123.103:1103")
parser.add_argument("file", help="File containing the list of files", default="files.txt")
args = parser.parse_args()

file_registry = args.file
bs = tuple(args.addrbs.split(":"))
bs = Node(bs[0], int(bs[1]))
me = args.addrme.split(":")
me = Node(me[0], int(me[1]), args.name)

def get_file_list(filename):
    '''
    Helper function to get the file list in the node from the file registry at `name`
    Args:
        name        : registry file name
    Returns:
        list(str)   : list of file names in the current node
    Raises:
        RuntimeError: if there is no registry file named `name`
    '''
    with open(filename, "r") as f:
        l = f.read().strip().split("\n")
        from random import shuffle
        shuffle(l)
        from random import randrange
        l = l[:randrange(3, 6)]
        for i in range(len(l)):
            l[i] = l[i].replace("\r","")
        return l
    raise RuntimeError("Error reading file")

with BootstrapServerConnection(bs, me) as conn:
    users = conn.users
    files = get_file_list(file_registry)
    with DistributedServer(users, me, files) as server:
        thread.start_new_thread(server.serve, ())
        while(True):
            print("Enter file name to search")
            filename = raw_input()
            filename = filename.strip()
            if filename != "":
                send_search(users, filename, me)
