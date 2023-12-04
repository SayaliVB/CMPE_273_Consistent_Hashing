import pyarrow as pa
import pyarrow.flight as flight
from hash_ring import HashRing
import threading


class GatewayClient:
    def __init__(self, port, host = 'localhost'):
        self.location = flight.Location.for_grpc_tcp(host, port)
        self.connection = flight.connect(self.location)
        self.connection.wait_for_available()
    
    def put_table(self, name, table):
        table_name = name
        descriptor = flight.FlightDescriptor.for_command(table_name)
        writer, reader = self.connection.do_put(descriptor, table.schema)
        writer.write(table)
        writer.close()
    
    def get_table(self, name):
        table_name = name.encode('utf8')
        ticket = flight.Ticket(table_name)
        reader = self.connection.do_get(ticket)
        return reader.read_all()


class Gateway(flight.FlightServerBase):
  def __init__(self, location, server_locations=set()):
    super(Gateway, self).__init__(location)
    self.server_locations = server_locations
    self.hr = HashRing()
    thread = threading.Thread(target=self.run_health_check())
    thread.start()

  def add_server(self, server):
    self.hr.add_node(server)

  def remove_server(self, server):
    self.hr.remove_node(server)
  
  def health_check(self, server):
    try:
      action = flight.Action('health_check', b'')
      client = flight.FlightClient(server)
      results = client.do_action(action)
      for result in results:
        if server not in self.hr.nodes:
          self.add_server(server)
        print(f"Server: {server} is healthy")
        return
      print(f"Health check for {server} passed, but server didn't respond as expected")
      return
    except Exception as e:
      self.remove_server(server)
      print(f"Health check failed for server: {server} with error: {e}")
      return

  def run_health_check(self,):
    threading.Timer(30.0, self.run_health_check).start()
    for server in self.server_locations:
      self.health_check(server)

  def do_put(self, context, descriptor, reader, 
                writer):
        
    #get data from apache client
    table_name = descriptor.command
    print("Table_name: ")
    print(table_name)
    table = reader.read_all()
    print("table:")
    print(table)

    # Determine the server to forward the data
    target_server = self.hr.add_key(table_name)
    client = GatewayClient(8816)
    
    #send to server
    client.put_table(table_name, table)

  def do_get(self, context, ticket):
    table_name = ticket.ticket
    table = self.tables[table_name]
    return flight.RecordBatchStream(table)

if __name__ == "__main__":
  # Server locations (replace with actual server addresses)
  #servers = ["grpc://localhost:8816", "grpc://localhost:8817", "grpc://localhost:8818"]
  servers = ["grpc://localhost:8816"]

  # Start the gateway server
  gateway = Gateway("grpc://localhost:8815", servers)
  print("Starting the gateway...")
  gateway.serve()