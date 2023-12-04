import pyarrow.flight as flight
import pyarrow as pa

class GatewayClient:
    def __init__(self, host = 'localhost', port = 8080):
        self.location = flight.Location.for_grpc_tcp(host, port)
        self.connection = flight.connect(self.location)
        self.connection.wait_for_available()
        print("Avaibale")
    
    def put_table(self, name, table):
        table_name = name
        descriptor = flight.FlightDescriptor.for_command(table_name)
        writer, reader = self.connection.do_put(descriptor, table.schema)
        writer.write(table)
        writer.close()
    
    def get_table(self, name):
        table_name = name
        ticket = flight.Ticket(table_name)
        reader = self.connection.do_get(ticket)
        return reader

class GatewayServer(flight.FlightServerBase):
  
    def __init__(self, 
                 host = 'localhost', 
                 port = 8081):
        self.location = flight.Location.for_grpc_tcp(host, port)
        super().__init__(self.location)
        self.client = GatewayClient()
      
    def do_put(self, context, descriptor, reader, 
               writer):
        
        #get data from apache client
        table_name = descriptor.command
        print("Table_name: ")
        print(table_name)
        table = reader.read_all()
        print("table:")
        print(table)
        
        #send to server
        self.client.put_table(table_name, table)
    
    def do_get(self, context, ticket):
        table_name = ticket.ticket
        return flight.RecordBatchStream(self.client.get_table(table_name).read_all())

server = GatewayServer()
print("Starting server1 at 8081...")

server.serve()
