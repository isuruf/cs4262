#
# Autogenerated by Thrift Compiler (0.9.3)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TException, TApplicationException
import logging
from ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class Iface:
  def join(self, me):
    """
    Parameters:
     - me
    """
    pass

  def leave(self, me):
    """
    Parameters:
     - me
    """
    pass

  def search(self, filename, requestor, hops, uuid):
    """
    Parameters:
     - filename
     - requestor
     - hops
     - uuid
    """
    pass

  def found_file(self, files, n, uuid):
    """
    Parameters:
     - files
     - n
     - uuid
    """
    pass


class Client(Iface):
  def __init__(self, iprot, oprot=None):
    self._iprot = self._oprot = iprot
    if oprot is not None:
      self._oprot = oprot
    self._seqid = 0

  def join(self, me):
    """
    Parameters:
     - me
    """
    self.send_join(me)
    return self.recv_join()

  def send_join(self, me):
    self._oprot.writeMessageBegin('join', TMessageType.CALL, self._seqid)
    args = join_args()
    args.me = me
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_join(self):
    iprot = self._iprot
    (fname, mtype, rseqid) = iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      raise x
    result = join_result()
    result.read(iprot)
    iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    raise TApplicationException(TApplicationException.MISSING_RESULT, "join failed: unknown result")

  def leave(self, me):
    """
    Parameters:
     - me
    """
    self.send_leave(me)
    return self.recv_leave()

  def send_leave(self, me):
    self._oprot.writeMessageBegin('leave', TMessageType.CALL, self._seqid)
    args = leave_args()
    args.me = me
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_leave(self):
    iprot = self._iprot
    (fname, mtype, rseqid) = iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      raise x
    result = leave_result()
    result.read(iprot)
    iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    raise TApplicationException(TApplicationException.MISSING_RESULT, "leave failed: unknown result")

  def search(self, filename, requestor, hops, uuid):
    """
    Parameters:
     - filename
     - requestor
     - hops
     - uuid
    """
    self.send_search(filename, requestor, hops, uuid)

  def send_search(self, filename, requestor, hops, uuid):
    self._oprot.writeMessageBegin('search', TMessageType.ONEWAY, self._seqid)
    args = search_args()
    args.filename = filename
    args.requestor = requestor
    args.hops = hops
    args.uuid = uuid
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()
  def found_file(self, files, n, uuid):
    """
    Parameters:
     - files
     - n
     - uuid
    """
    self.send_found_file(files, n, uuid)

  def send_found_file(self, files, n, uuid):
    self._oprot.writeMessageBegin('found_file', TMessageType.ONEWAY, self._seqid)
    args = found_file_args()
    args.files = files
    args.n = n
    args.uuid = uuid
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

class Processor(Iface, TProcessor):
  def __init__(self, handler):
    self._handler = handler
    self._processMap = {}
    self._processMap["join"] = Processor.process_join
    self._processMap["leave"] = Processor.process_leave
    self._processMap["search"] = Processor.process_search
    self._processMap["found_file"] = Processor.process_found_file

  def process(self, iprot, oprot):
    (name, type, seqid) = iprot.readMessageBegin()
    if name not in self._processMap:
      iprot.skip(TType.STRUCT)
      iprot.readMessageEnd()
      x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
      oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
      x.write(oprot)
      oprot.writeMessageEnd()
      oprot.trans.flush()
      return
    else:
      self._processMap[name](self, seqid, iprot, oprot)
    return True

  def process_join(self, seqid, iprot, oprot):
    args = join_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = join_result()
    try:
      result.success = self._handler.join(args.me)
      msg_type = TMessageType.REPLY
    except (TTransport.TTransportException, KeyboardInterrupt, SystemExit):
      raise
    except Exception as ex:
      msg_type = TMessageType.EXCEPTION
      logging.exception(ex)
      result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
    oprot.writeMessageBegin("join", msg_type, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_leave(self, seqid, iprot, oprot):
    args = leave_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = leave_result()
    try:
      result.success = self._handler.leave(args.me)
      msg_type = TMessageType.REPLY
    except (TTransport.TTransportException, KeyboardInterrupt, SystemExit):
      raise
    except Exception as ex:
      msg_type = TMessageType.EXCEPTION
      logging.exception(ex)
      result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
    oprot.writeMessageBegin("leave", msg_type, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_search(self, seqid, iprot, oprot):
    args = search_args()
    args.read(iprot)
    iprot.readMessageEnd()
    try:
      self._handler.search(args.filename, args.requestor, args.hops, args.uuid)
      msg_type = TMessageType.REPLY
    except (TTransport.TTransportException, KeyboardInterrupt, SystemExit):
      raise
    except:
      pass

  def process_found_file(self, seqid, iprot, oprot):
    args = found_file_args()
    args.read(iprot)
    iprot.readMessageEnd()
    try:
      self._handler.found_file(args.files, args.n, args.uuid)
      msg_type = TMessageType.REPLY
    except (TTransport.TTransportException, KeyboardInterrupt, SystemExit):
      raise
    except:
      pass


# HELPER FUNCTIONS AND STRUCTURES

class join_args:
  """
  Attributes:
   - me
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'me', (Node, Node.thrift_spec), None, ), # 1
  )

  def __init__(self, me=None,):
    self.me = me

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.me = Node()
          self.me.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('join_args')
    if self.me is not None:
      oprot.writeFieldBegin('me', TType.STRUCT, 1)
      self.me.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.me)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class join_result:
  """
  Attributes:
   - success
  """

  thrift_spec = (
    (0, TType.I16, 'success', None, None, ), # 0
  )

  def __init__(self, success=None,):
    self.success = success

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.I16:
          self.success = iprot.readI16()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('join_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.I16, 0)
      oprot.writeI16(self.success)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.success)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class leave_args:
  """
  Attributes:
   - me
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'me', (Node, Node.thrift_spec), None, ), # 1
  )

  def __init__(self, me=None,):
    self.me = me

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.me = Node()
          self.me.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('leave_args')
    if self.me is not None:
      oprot.writeFieldBegin('me', TType.STRUCT, 1)
      self.me.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.me)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class leave_result:
  """
  Attributes:
   - success
  """

  thrift_spec = (
    (0, TType.I16, 'success', None, None, ), # 0
  )

  def __init__(self, success=None,):
    self.success = success

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.I16:
          self.success = iprot.readI16()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('leave_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.I16, 0)
      oprot.writeI16(self.success)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.success)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class search_args:
  """
  Attributes:
   - filename
   - requestor
   - hops
   - uuid
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'filename', None, None, ), # 1
    (2, TType.STRUCT, 'requestor', (Node, Node.thrift_spec), None, ), # 2
    (3, TType.I16, 'hops', None, None, ), # 3
    (4, TType.STRING, 'uuid', None, None, ), # 4
  )

  def __init__(self, filename=None, requestor=None, hops=None, uuid=None,):
    self.filename = filename
    self.requestor = requestor
    self.hops = hops
    self.uuid = uuid

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.filename = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRUCT:
          self.requestor = Node()
          self.requestor.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I16:
          self.hops = iprot.readI16()
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.STRING:
          self.uuid = iprot.readString()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('search_args')
    if self.filename is not None:
      oprot.writeFieldBegin('filename', TType.STRING, 1)
      oprot.writeString(self.filename)
      oprot.writeFieldEnd()
    if self.requestor is not None:
      oprot.writeFieldBegin('requestor', TType.STRUCT, 2)
      self.requestor.write(oprot)
      oprot.writeFieldEnd()
    if self.hops is not None:
      oprot.writeFieldBegin('hops', TType.I16, 3)
      oprot.writeI16(self.hops)
      oprot.writeFieldEnd()
    if self.uuid is not None:
      oprot.writeFieldBegin('uuid', TType.STRING, 4)
      oprot.writeString(self.uuid)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.filename)
    value = (value * 31) ^ hash(self.requestor)
    value = (value * 31) ^ hash(self.hops)
    value = (value * 31) ^ hash(self.uuid)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class found_file_args:
  """
  Attributes:
   - files
   - n
   - uuid
  """

  thrift_spec = (
    None, # 0
    (1, TType.LIST, 'files', (TType.STRING,None), None, ), # 1
    (2, TType.STRUCT, 'n', (Node, Node.thrift_spec), None, ), # 2
    None, # 3
    (4, TType.STRING, 'uuid', None, None, ), # 4
  )

  def __init__(self, files=None, n=None, uuid=None,):
    self.files = files
    self.n = n
    self.uuid = uuid

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.LIST:
          self.files = []
          (_etype3, _size0) = iprot.readListBegin()
          for _i4 in xrange(_size0):
            _elem5 = iprot.readString()
            self.files.append(_elem5)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRUCT:
          self.n = Node()
          self.n.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.STRING:
          self.uuid = iprot.readString()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('found_file_args')
    if self.files is not None:
      oprot.writeFieldBegin('files', TType.LIST, 1)
      oprot.writeListBegin(TType.STRING, len(self.files))
      for iter6 in self.files:
        oprot.writeString(iter6)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.n is not None:
      oprot.writeFieldBegin('n', TType.STRUCT, 2)
      self.n.write(oprot)
      oprot.writeFieldEnd()
    if self.uuid is not None:
      oprot.writeFieldBegin('uuid', TType.STRING, 4)
      oprot.writeString(self.uuid)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.files)
    value = (value * 31) ^ hash(self.n)
    value = (value * 31) ^ hash(self.uuid)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
