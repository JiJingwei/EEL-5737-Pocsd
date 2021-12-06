import pickle, logging
import argparse
import hashlib

# For locks: RSM_UNLOCKED=0 , RSM_LOCKED=1 
RSM_UNLOCKED = bytearray(b'\x00') * 1
RSM_LOCKED = bytearray(b'\x01') * 1

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
  rpc_paths = ('/RPC2',)

class DiskBlocks():
  def __init__(self, total_num_blocks, block_size):
    # This class stores the raw block array
    self.block = []
    self.checksum = []
    # Initialize raw blocks 
    for i in range (0, total_num_blocks):
      putdata = bytearray(block_size)
      putchecksum = hashlib.md5(str(putdata).encode('utf-8')).digest()
      self.block.insert(i,putdata)
      self.checksum.insert(i,putchecksum)


if __name__ == "__main__":

  # Construct the argument parser
  ap = argparse.ArgumentParser()

  ap.add_argument('-nb', '--total_num_blocks', type=int, help='an integer value')
  ap.add_argument('-bs', '--block_size', type=int, help='an integer value')
  ap.add_argument('-port', '--port', type=int, help='an integer value')
  ap.add_argument('-sid', '--server_id',type=int,help='an integer value')
  ap.add_argument('-cblk', '--corrupted_block',type=int,help='an integer value')

  args = ap.parse_args()

  if args.total_num_blocks:
    TOTAL_NUM_BLOCKS = args.total_num_blocks
  else:
    print('Must specify total number of blocks') 
    quit()

  if args.block_size:
    BLOCK_SIZE = args.block_size
  else:
    print('Must specify block size')
    quit()

  if args.port:
    PORT = args.port
  else:
    print('Must specify port number')
    quit()

  if args.server_id>=0:
    server_id = args.server_id
  else:
    print('Must specify correct server id')
    quit()

  if args.corrupted_block:
    corrupted_block = args.corrupted_block
  else:
    corrupted_block = -1

  # initialize blocks
  RawBlocks = DiskBlocks(TOTAL_NUM_BLOCKS, BLOCK_SIZE)
  #Corrupted = False
  # Create server

  server = SimpleXMLRPCServer(("127.0.0.1", PORT), requestHandler=RequestHandler)

  def Get(block_number):
    if hashlib.md5(str(RawBlocks.block[block_number]).encode('utf-8')).digest() != RawBlocks.checksum[block_number]:
      have_error = True
    else:
      have_error = False
    if block_number == corrupted_block:
      return bytearray(BLOCK_SIZE),True

    result = RawBlocks.block[block_number]
    return result,have_error

  server.register_function(Get)

  def Put(block_number, data):
    RawBlocks.block[block_number] = data
    RawBlocks.checksum[block_number] = hashlib.md5(str(data).encode('utf-8')).digest()
    return 0

  server.register_function(Put)

  def RSM(block_number):
    result = RawBlocks.block[block_number]
    # RawBlocks.block[block_number] = RSM_LOCKED
    RawBlocks.block[block_number] = bytearray(RSM_LOCKED.ljust(BLOCK_SIZE,b'\x01'))
    return result

  server.register_function(RSM)

  # Run the server's main loop
  print ("Running block server with nb=" + str(TOTAL_NUM_BLOCKS) + ", bs=" + str(BLOCK_SIZE) + " on port " + str(PORT))
  server.serve_forever()

