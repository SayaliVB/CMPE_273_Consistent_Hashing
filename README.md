# consistent_hashing_cmpe_273
Group project to implement a consistent hashing system using Apache Arrow Flight for communication


<details>
  <summary>apache_gateway.py</summary>

  It acts as a gateway to implement consistent hashing for storing data throughout multiple servers.
  It contains an object for hashring that add/removes nodes and to to the hasing ring and gives ot the location of virtual for a give key_hash.

</details>

<details>
  <summary>apache_server.py</summary>

  Server that stores the tables sent from the gateway.

</details>

<details>
  <summary>apache_client.py</summary>

  Client to send data to the servers via gateway.

</details>

<details>
  <summary>hash_ring.py</summary>

  Implementation of consitent hashing.
  Replication to enhance fault tolerance.
  Collision control implemented using LinkedList

</details>

<details>
  <summary>Important Links: </summary>
  
  https://blog.djnavarro.net/posts/2022-10-18_arrow-flight/
  https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightStreamReader.html#pyarrow-flight-flightstreamreader
  https://medium.com/@diehardankush/how-to-making-sense-of-apache-arrow-flight-8ca595e2c3f6
  
</details>

<details>
  <summary>Group repository: </summary>
  subramanyaJagadeesh/consistent_hashing_cmpe_273
</details>


