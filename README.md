# Distributed Cloud Storage Service

Distributed and replicated key-value (KV) storage service created as the course project for ECE419: Distributed Systems. The KV pairs are consistently hashed using a hashring and distributed among the running storage servers. The addition or removal of a storage server triggers the transfer of data between storage servers to maintain consistent hashing on the hashring, allowing the system to scale appropiately. A KV Pair is replicated onto two additional storage servers (from the original "primary" storage server) to ensure data is not lost when storage servers unexpectedly shutdown. The data transfer operations, sharing of metadata, and data replication is facilitated by an External Configuration Service (ECS) to increase availability of the storage servers. Messages between the clients, storage servers, and ECS are encrypted using public-key cryptography to keep data confidential.

## Quick Start

### 1. Install Java and OpenJDK

Download and install Java if not done already: https://www.java.com/en/download/help/download_options.html

Download and install OpenJDK:
(Microsoft) https://learn.microsoft.com/en-us/java/openjdk/install

NOTE: The project was created with Java 8, new versions of Java are not compatible. Installing OpenJDK seems to also work.

### 2. Initialize the External Configuration Service (ECS)

`java -jar .\m3-ecs.jar -a localhost -p 1234`

### 3. Start a new storage server

In a new terminal tab, run the following command:

`java -jar .\m3-server.jar -a localhost -p 1111 -b localhost:1234`

-b stands for bootstrap and contains the ECS IP address and port

You should see a new directory titled "data" with the file 1111.properties in it.

### 4. Start a client connection

In a new terminal tab, run the following command:

`java -jar .\m3-client.jar`

### 4. Connect the client to the storage server

In the client terminal prompt, enter this command to connect to the storage server on port 1111:

`connect localhost 1111`

### 5. Add, Update, and Delete Key-Value Pairs from the Storage Server

To add a key-value (KV) pair to the storage server, enter this command into the client terminal:

`put key1 value1`

You should see this KV pair in the /data/1111.properties file.

To update a value, it's the same command, but with a new value:

`put key1 321`

To read a key's value, use the get command:

`get key1`

Finally, to delete a KV pair, use a put command with no value:

`put key1`

## Further Testing

Here are some additional tests to see more functionality:

### Data Replication

1. Start the ECS: `java -jar .\m3-ecs.jar -a localhost -p 1234`
2. Start 5 storage servers: `java -jar .\m3-server.jar -a localhost -p 1111 -b localhost:1234` using a different port number for each server (i.e. -p 2222, -p 3333, etc.)
3. Start a client terminal (`java -jar .\m3-client.jar`) and connect to any one of the storage servers (`connect localhost port_number_of_storage_server`)
4. Begin adding key-value pairs with `put key value`. Notice each KV pair appears in 3 (1 leader, 2 replicants) of the storage property files in the /data/ directory. A hashring is used to decide which storage servers are responsible for each KV pair
5. Try updating (`put key new_value`) and deleting (`put key`) KV pairs. Notice the changes are applied to all 3 property files that are responsible for the KV pair.

### Storage Server Failure Handling

Similar to the previous test, start the ECS, then 5 storage servers. Add some KV pairs. Find the process id of one of the storage servers, and kill it (`kill -9 pid_number`). The ECS will catch the failure using a shutdown hook and proceed with reallocating KV pairs and replications (look in the /data/ directory files)
