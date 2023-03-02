package app_kvECS;

import java.util.Map;
import java.util.TreeMap;
import java.io.IOException;
import java.math.BigInteger;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.text.ParseException;
import java.util.Collection;
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

public class ECSClient extends Thread implements IECSClient {

    private static Logger logger = Logger.getRootLogger();
    private ServerSocket ECSServerSocket;
	private boolean running;
    private String addr;
    private int port;
    private TreeMap<String, String> metadata;


    public ECSClient(String addr, int port) {
		this.port = port;
        this.addr = addr;
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
        // MessageDigest md = MessageDigest.getInstance("MD5");
        // byte[] MD5digest = md.digest(input_str.getBytes());

        // return new BigInteger(1, MD5digest);
        return DigestUtils.md5Hex(input_str);
    }

    private void addToMetaData(String server_ip_port) {
        String hash_value = hash(server_ip_port);

        metadata.put(hash_value, server_ip_port);
        logger.info("Added " + server_ip_port + " to metadata.");
    }

    private void updateMetaData() {
        
    }

    private void removeFromMetaData(String server_ip_port) {
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
					KVServerConnection connection = new KVServerConnection(kvServer, this);
					new Thread(connection).start();

					logger.info("Connected to "
							+ kvServer.getInetAddress().getHostName()
							+ " on port " + kvServer.getPort() + "\n");
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("ECS Server stopped.");
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

    public static void main(String[] args) {
		String address_str;
		int port_int;
        
        System.out.println(args[2].equals("-p"));
		if ((args.length == 4) && (args[0].equals("-a")) && (args[2].equals("-p"))) {
			try {
                new LogSetup("logs/ecs.log", Level.ALL);
            } catch (IOException e) {
                System.out.println("Error! Unable to initialize logger!");
                e.printStackTrace();
                System.exit(1);
            }
            address_str = args[1];
			port_int = Integer.parseInt(args[3]);

			new ECSClient(address_str, port_int).start();
		} else {
			System.out.println("Error! Incorrect arguments. Expected -b <ecs_ip>:<ecs_address> -a <address> -p <port>");
		}
        // Options options = new Options();

        // Option address = new Option("a", "address", true, "ECS IP adress");
        // address.setRequired(true);
        // options.addOption(address);

        // Option port = new Option("p", "port", true, "ECS Port");
        // port.setRequired(true);
        // options.addOption(port);

        // CommandLineParser parser = new DefaultParser();
        // HelpFormatter formatter = new HelpFormatter();
        // CommandLine cmd = null;
        // try {
		// 	new LogSetup("logs/ecs.log", Level.ALL);
        //     cmd = parser.parse(options, args);
		// } catch (ParseException e) {
        //     System.out.println(e.getMessage());
		// 	formatter.printHelp("utility-name", options);
		// 	System.exit(1);
        // }
        // catch (IOException e) {
		// 	System.out.println("Error! Unable to initialize logger!");
		// 	e.printStackTrace();
		// 	System.exit(1);
		// } 

        // String address_str = cmd.getOptionValue("address");
        // int port_int = Integer.parseInt(cmd.getOptionValue("port"));

        // new ECSClient(address_str, port_int).start();
    }
}
