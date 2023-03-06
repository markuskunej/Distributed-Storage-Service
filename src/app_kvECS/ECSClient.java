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

public class ECSClient extends Thread implements IECSClient {

    private static Logger logger = Logger.getRootLogger();
    private ServerSocket ECSServerSocket;
	private boolean running;
    private String addr;
    private int port;
    private TreeMap<String, String> metadata;
    //private HashMap<String, Socket> socketMap = new HashMap<>();
    private HashMap<String, KVServerConnection> connectionMap = new HashMap<>();



    public ECSClient(String addr, int port) {
		this.port = port;
        this.addr = addr;
        metadata = new TreeMap<String, String>();
	}


    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    public void addServer(String server_name) {
        addToMetaData(server_name);
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    private String hash(String input_str) {
        return DigestUtils.md5Hex(input_str);
    }

    // public void addToConnections(String server_name, KVServerConnection conn) {
    //     connectionMap.put(server_name, conn);
    // }
    public void removeFromConnections(String key) {
        logger.info("removing " + key + " from connectionsMap.");
        connectionMap.remove(key);
        logger.info("New Connection Map is: " + connectionMap.toString());
    }

    public void updateConnectionMap(String oldKey, String newKey) {
        KVServerConnection conn = connectionMap.remove(oldKey);
        if (conn == null) {
            logger.error("ERROR! Cannot update the connection map because the old key had no connection value");
        } else {
            connectionMap.put(newKey, conn);
            logger.info("New Connection Map is: " + connectionMap.toString());
        }
    }

    public void addToMetaData(String server_ip_port) {
        String hash_value = hash(server_ip_port);
        logger.info("hash is " + hash_value);
        this.metadata.put(hash_value, server_ip_port);
        logger.info("Added " + server_ip_port + " to metadata.");
    }

    public void updateMetaDatas() {
        ECSMessage metadata_msg = new ECSMessage(metadata, StatusType.METADATA);
        for (Map.Entry<String, KVServerConnection> conn_entry : connectionMap.entrySet()) {
            try {
                conn_entry.getValue().sendMessage(metadata_msg);
                //sendMessage(sock_entry.getValue(), metadata_msg);
            } catch (IOException ioe) {
                logger.error("Error! Unable to send metadata update to server: " + conn_entry.getKey());
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
        String serverBefore = metadata.lowerKey(hash(newServer));
        if (serverBefore == null) {
            //this new server has the lowest value in hash ring, successor is the server with largest hash key
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
                    //socketMap.put(serverName, kvServer);
					KVServerConnection connection = new KVServerConnection(kvServer, this, tempName);
					//String serverName = kvServer.getInetAddress().getHostAddress() + ":" + kvServer.getLocalPort();
                    logger.info("temp name is " + tempName);
                    //addToMetaData(serverName);
                    connectionMap.put(connection.getServerName(), connection);	
                    logger.info("New Connection Map is: " + connectionMap.toString());				

                    new Thread(connection).start();
                    //connectionMap.put(connection.getServerName(), connection);	
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
    public void invokeTransferTo(String srcServer, String destServer) {
        logger.info("srcServer is " + srcServer);
        logger.info("connectioMap is " + connectionMap.toString());
        KVServerConnection connection = connectionMap.get(srcServer);
        //Socket successorSocket = socketMap.get(srcServer);
        logger.info("invokeTransferTo before if " + connection);

        if (connection != null) {
            try {
                logger.info("invokeTransferTo");
                connection.sendMessage(new ECSMessage(destServer, StatusType.TRANSFER_TO_REQUEST));
            } catch (IOException ioe) {
                logger.error("Error! Unable to send TRANSFER_TO_REQUEST message to successor server");
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
		// String address_str;
		// int port_int;
        
        // System.out.println(args[2].equals("-p"));
		// if ((args.length == 6) && (args[0].equals("-a")) && (args[2].equals("-p")) && (args[4].equals("-ll"))) {
		// 	try {
        //         new LogSetup("logs/ecs.log", Level.ALL);
        //     } catch (IOException e) {
        //         System.out.println("Error! Unable to initialize logger!");
        //         e.printStackTrace();
        //         System.exit(1);
        //     }
        //     address_str = args[1];
		// 	port_int = Integer.parseInt(args[3]);

		// 	new ECSClient(address_str, port_int).start();
		// } else {
		// 	System.out.println("Error! Incorrect arguments. Expected -b <ecs_ip>:<ecs_address> -a <address> -p <port> -ll <logLevel>");
		// }
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
