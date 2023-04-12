package app_kvECS;

import java.util.Map;
import java.util.TreeMap;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.text.ParseException;
import java.util.Collection;
import java.util.HashMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.cli.*;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import ecs.IECSNode;
import ecs.KVServerConnection;
import logger.LogSetup;
import shared.messages.ECSMessage;
import shared.messages.IECSMessage.StatusType;

public class ECSClient extends Thread {

    private static Logger logger = Logger.getRootLogger();
    private ServerSocket ECSServerSocket;
	private boolean running;
    private String addr;
    private int port;
    private TreeMap<String, String> metadata;
    private HashMap<String, KVServerConnection> connectionMap = new HashMap<>();

    public ECSClient(String addr, int port) {
		this.port = port;
        this.addr = addr;
        metadata = new TreeMap<String, String>();
	}

    public void addServer(String server_name) {
        addToMetaData(server_name);
    }

    private String hash(String input_str) {
        return DigestUtils.md5Hex(input_str);
    }

    public void removeFromConnections(String key) {
        connectionMap.remove(key);
    }

    public void updateConnectionMap(String oldKey, String newKey) {
        KVServerConnection conn = connectionMap.remove(oldKey);
        if (conn == null) {
            logger.error("ERROR! Cannot update the connection map because the old key had no connection value");
        } else {
            connectionMap.put(newKey, conn);
        }
    }

    public void addToMetaData(String server_ip_port) {
        String hash_value = hash(server_ip_port);
        this.metadata.put(hash_value, server_ip_port);
        logger.info("Added " + server_ip_port + " to metadata.");
    }

    public void updateMetaDatas() {
        if (metadata.size() != 0) {
            ECSMessage metadata_msg = new ECSMessage(metadata, StatusType.METADATA);
            for (Map.Entry<String, KVServerConnection> conn_entry : connectionMap.entrySet()) {
                try {
                    conn_entry.getValue().sendMessage(metadata_msg);
                } catch (IOException ioe) {
                    logger.error("Error! Unable to send metadata update to server: " + conn_entry.getKey());
                }
            }
        }
    }

    public TreeMap<String, String> getMetaData() {
        return metadata;
    }

    public String getSuccesorServer(String newServer) {
        if (metadata.size() == 1) {
            // only server connected to ECS, no successor
            return null;
        }
        String serverAfter = metadata.higherKey(hash(newServer));
        if (serverAfter == null) {
            //this new server has the highest value in hash ring, successor is the server with lowest hash key
            return metadata.get(metadata.firstKey());
        } else {
            return metadata.get(serverAfter);
        }
    }

    public String getPredecessorServer(String newServer) {
        if (metadata.size() == 1) {
            // only server connected to ECS, no successor
            return null;
        }
        String serverBefore = metadata.lowerKey(hash(newServer));
        if (serverBefore == null) {
            //this new server has the lowest value in hash ring, successor is the server with lowest hash key
            return metadata.get(metadata.lastKey());
        } else {
            return metadata.get(serverBefore);
        }
    }

    public void removeFromMetaData(String server_ip_port) {
        String hash_value = hash(server_ip_port);
        
        String prev_value = metadata.remove(hash_value);

        if (prev_value.equals(server_ip_port)) {
            logger.info("Successfully removed " + server_ip_port + " from metadata");
        } else {
            logger.error("Error! Can not remove " + server_ip_port + " from metadata since it doesn't exist!");
        }
    }

    private boolean isRunning() {
        return this.running;
    }

    public void run() {

		running = initializeECS();

		if (ECSServerSocket != null) {   
			while (isRunning()) {
				try {
					Socket kvServer = ECSServerSocket.accept();
                    String serverName = kvServer.getInetAddress().getHostAddress() + ":" + kvServer.getPort();
                    String tempName = Integer.toString(kvServer.getPort());
					KVServerConnection connection = new KVServerConnection(kvServer, this, tempName);

                    connectionMap.put(connection.getServerName(), connection);	

                    new Thread(connection).start();
                    logger.info("Connected to "
							+ kvServer.getInetAddress().getHostName()
							+ " on port " + kvServer.getLocalPort() + "\n");

                    

				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("ECS Server stopped.");
	}

    // send a message to the server that will be transferring data to another server
    public void invokeTransferTo(String srcServer, String destServer, String coordinatorServer) {
        KVServerConnection connection = connectionMap.get(srcServer);
        String servers = coordinatorServer + ',' + destServer;
        if (connection != null) {
            try {
                logger.info("Invoking transfer of KV pairs between servers...");
                connection.sendMessage(new ECSMessage(servers, StatusType.TRANSFER_TO_REQUEST));
            } catch (IOException ioe) {
                logger.error("Error! Unable to send TRANSFER_TO_REQUEST message to successor server");
            }
        }
    }

    public void invokeTransferAllTo(String srcServer, String destServer) {
        KVServerConnection connection = connectionMap.get(srcServer);
        if (connection != null) {
            try {
                logger.info("Invoking transfer of all KV pairs between servers...");
                connection.sendMessage(new ECSMessage(destServer, StatusType.TRANSFER_ALL_TO_REQUEST));
            } catch (IOException ioe) {
                logger.error("Error! Unable to send TRANSFER_ALL_TO_REQUEST message to successor server");
            }
        }
    }

    public void newServerTransfers(String new_server_name) {
        try {
            logger.info("connectionMap size is " + connectionMap.size());
            if (connectionMap.size() == 1) {
                // first server, no need for transfer
                return;
            } else if (connectionMap.size() <= 3) {
                // special case where where each server should have all the same data
                // since it'll be a replica of the other 2 + its own coordinator items
                String successorName = getSuccesorServer(new_server_name);
                KVServerConnection connection = connectionMap.get(successorName);
                connection.sendMessage(new ECSMessage(new_server_name, StatusType.TRANSFER_ALL_TO_REQUEST));
            } else {
                // more than 3 connections active, requires a re-shuffling of data
                // get next 3 successors
                String[] successors = new String[3];
                successors[0] = getSuccesorServer(new_server_name);
                successors[1] = getSuccesorServer(successors[0]);
                successors[2] = getSuccesorServer(successors[1]);

                // get 2 predecessors (+ new server). Note the reverse filling of the array
                String[] predecessors = new String[3];
                predecessors[2] = new_server_name;
                predecessors[1] = getPredecessorServer(new_server_name);
                predecessors[0] = getPredecessorServer(predecessors[1]);

                // each of the successors no longer replicates one of the predecessors,
                // transfer those replicated items to the new server
                for (int i=0; i < 3; i++) {
                    invokeTransferTo(successors[i], new_server_name, predecessors[i]);
                }
            }
        } catch (IOException ioe) {
            logger.error("ERROR in newServerTransfer.\n", ioe);
        }
    }

    public void removeServerTransfer(String removed_server) {
        if (connectionMap.size() < 3) {
            // special case where where each server should have all the same data
            // since it'll be a replica of the other 2 + its own coordinator items
            logger.info("IN REMOVE SERVER TRANSFER");
            logger.info(connectionMap.size());
        } else if (connectionMap.size() == 3) {
            // special case where all servers should have same data
            // similar as case after, but use TRANSFER_ALL instead of TRANSFER_TO
            // since it won't delete the kv pairs afterwards
            String[] successors = new String[3];
            successors[0] = getSuccesorServer(removed_server);
            successors[1] = getSuccesorServer(successors[0]);
            successors[2] = getSuccesorServer(successors[1]);

            // get 2 predecessors (+ succesor). Note the reverse filling of the array
            String[] predecessors = new String[3];
            predecessors[2] = successors[0];
            predecessors[1] = getPredecessorServer(removed_server);
            predecessors[0] = getPredecessorServer(predecessors[1]);

            // transfer all
            for (int i=0; i < 3; i++) {
                invokeTransferAllTo(predecessors[i], successors[i]);
            }

        } else {
            // more than 3 connections active, requires a re-shuffling of data
            // get next 3 successors
            String[] successors = new String[3];
            successors[0] = getSuccesorServer(removed_server);
            successors[1] = getSuccesorServer(successors[0]);
            successors[2] = getSuccesorServer(successors[1]);

            // get 2 predecessors (+ succesor). Note the reverse filling of the array
            String[] predecessors = new String[3];
            predecessors[2] = successors[0];
            predecessors[1] = getPredecessorServer(removed_server);
            predecessors[0] = getPredecessorServer(predecessors[1]);

            // each of the successors no longer replicates one of the predecessors,
            // transfer those replicated items to the new server
            for (int i=0; i < 3; i++) {
                invokeTransferTo(predecessors[i], successors[i], successors[i]);
            }
        }
    }

    // alert successor server of shutdown occurence
    public void updateMetaData(String serverName) {
        KVServerConnection connection = connectionMap.get(serverName);

        if (connection != null) {
            try {
                // send updated metadata to single server
                connection.sendMessage(new ECSMessage(metadata, StatusType.METADATA));
            } catch (IOException ioe) {
                logger.error("Error! Unable to send metadata update message to server: " + serverName);
            }
        }
    }

    private void sendMessage(Socket socket, ECSMessage msg) throws IOException {
        OutputStream output = socket.getOutputStream();
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<"
				+ socket.getInetAddress().getHostAddress() + ":"
				+ socket.getPort() + ">: '"
				+ msg.getMsg() + "'");
        output.close();        
    }

    private boolean initializeECS() {
		logger.info("Initialize ECS ... \n");
		try {
			ECSServerSocket = new ServerSocket();
            //bind to ip address and port
            ECSServerSocket.bind(new InetSocketAddress(addr, port));
			logger.info("ECS Server listening on address: " + ECSServerSocket.getInetAddress() + " port: "
					+ ECSServerSocket.getLocalPort() + "\n");
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket: \n");
			if (e instanceof BindException) {
				logger.error("Address: " + addr + " Port: " + port + " is already bound! \n");
			}
			return false;
		}
	}

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option address = new Option("a", "address", true, "ECS IP adress");
        address.setRequired(true);
        options.addOption(address);

        Option port = new Option("p", "port", true, "ECS Port");
        port.setRequired(true);
        options.addOption(port);

        Option logLevel = new Option("ll", "logLevel", true, "Log Level. Default is INFO");
        logLevel.setRequired(false);
        options.addOption(logLevel);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            //default values
			Level log_level = Level.INFO;
            cmd = parser.parse(options, args);
            if (cmd.hasOption("ll")) {
				log_level = Level.toLevel(cmd.getOptionValue("logLevel"));
			}            
			new LogSetup("logs/ecs.log", log_level);
            String address_str = cmd.getOptionValue("address");
            int port_int = Integer.parseInt(cmd.getOptionValue("port"));
            new ECSClient(address_str, port_int).start();            
		} catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
			formatter.printHelp("utility-name", options);
			System.exit(1);
        }
    }
}
