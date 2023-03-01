package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import server.KVClientConnection;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

public class KVServer extends Thread implements IKVServer {

	private static Logger logger = Logger.getRootLogger();

	private int port;
	private ServerSocket serverSocket;
	private boolean running;
	private int cacheSize;
	private CacheStrategy strategy;
	private String storage_file_path;
	private boolean stopped;

	/**
	 * Start KV Server at given port
	 * 
	 * @param port      given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *                  to keep in-memory
	 * @param strategy  specifies the cache replacement strategy in case the cache
	 *                  is full and there is a GET- or PUT-request on a key that is
	 *                  currently not contained in the cache. Options are "FIFO",
	 *                  "LRU",
	 *                  and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		// this.strategy = CacheStrategy.valueOf(strategy);
		this.strategy = CacheStrategy.None;
		this.storage_file_path = "src/data/kv.properties";

		initializeStorage();
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
	public String getHostname() {
		// try {
		// InetAddress sv = InetAddress.getLocalHost();

		// return sv.getHostName();
		// } catch (UnknownHostException ex) {
		// logger.error("Error! Unknown Host. \n", ex);

		// return null;
		// }
		return null;
	}

	@Override
	public CacheStrategy getCacheStrategy() {
		return this.strategy;
	}

	@Override
	public int getCacheSize() {
		return this.cacheSize;
	}

	@Override
	public boolean inStorage(String key) {
		// try {
		// // boolean exists = (storage.getKV(key) != null);
		// boolean exists = false;

		// if (exists) {
		// logger.info("Key :: " + key + " found in storage. \n");
		// } else {
		// logger.info("Key :: " + key + " not found in storage. \n");
		// }

		// return exists;

		// } catch (Exception e) {
		// logger.error("IO Failure. \n", e);

		// return false;
		// }
		return false;
	}

	@Override
	public boolean inCache(String key) {
		if (this.strategy == CacheStrategy.None) {
			return false;
		}

		// boolean exists = (cache.getKV(key) != null);
		boolean exists = false;
		if (exists) {
			logger.info("Key :: " + key + " found in cache. \n");
		} else {
			logger.info("Key :: " + key + " not found in cache. \n");
		}

		return exists;
	}

	@Override
	public String getKV(String key) throws Exception {
		try (InputStream input = new FileInputStream(storage_file_path)) {

			Properties prop = new Properties();

			// load the kv storage
			prop.load(input);

			// get value
			String value = prop.getProperty(key);

			if (value != null) {
				logger.info("Key :: " + key + ", Value :: " + value + "\n");
			} else {
				// value doesn't exist
				logger.info("Key :: " + key + " has no associated value. \n");
			}

			input.close();

			return value;

		} catch (Exception e) {
			logger.error("ERROR in KVServer.getKV \n", e);

			return null;
		}
	}

	@Override
	public StatusType putKV(String key, String value) throws Exception {
		try (InputStream input = new FileInputStream(storage_file_path)) {
			StatusType status;
			Properties prop = new Properties();

			prop.load(input);
			input.close();

			if ((value == null) || (value == "")) {
				// delete value from key
				Object prev_val = prop.remove(key);
				if (prev_val != null) {
					status = StatusType.DELETE_SUCCESS;
					logger.info("Deleting key-value pair from storage: Key :: " + key + ", Value :: " + prev_val.toString() + "\n");
				} else {
					status = StatusType.DELETE_ERROR;
					logger.info("Cannot delete key-value pair from storage, it doesn't exist" + "\n");
				}
			} else {
				Object prev_val = prop.setProperty(key, value);

				if (prev_val == null) {
					// Didn't exist before
					logger.info("Adding key-value pair to storage: Key :: " + key + ", Value :: " + value + "\n");
					status = StatusType.PUT_SUCCESS;
				} else {
					// Already exists, update instead
					logger.info("Updating key-value pair for Key :: " + key + ", from Value :: " + prev_val.toString()
							+ " to Value :: " + value + "\n");
					status = StatusType.PUT_UPDATE;
				}
			}

			OutputStream output = new FileOutputStream(storage_file_path);
			// store key-values back in file
			prop.store(output, null);
			output.close();

			return status;

		} catch (Exception e) {
			logger.error("IO Failure. \n", e);

			return StatusType.PUT_ERROR;
		}
	}

	@Override
	public void clearCache() {
		logger.info("Clear Cache. \n");
		if (this.strategy != CacheStrategy.None) {
			// cache.clear();
		}
	}

	@Override
	public void clearStorage() {
		try {
			clearCache();
			logger.info("Clear Storage. \n");
			// storage.clear();
		} catch (Exception e) {
			logger.error("Cannot clear Storage. \n", e);
		}
	}

	@Override
	public void run() {

		running = initializeServer();

		if (serverSocket != null) {
			while (isRunning()) {
				try {
					Socket client = serverSocket.accept();
					KVClientConnection connection = new KVClientConnection(client, this);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+ " on port " + client.getPort() + "\n");
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	@Override
	public void kill() {
		this.running = false;

		try {
			logger.info("Terminating Server. \n");
			// client.stop();
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! Termination failure on port: " + port, e);
		}
	}

	private boolean isRunning() {
		return this.running;
	}

	private boolean isStopped() {
		return this.stopped;
	}

	@Override
	public void close() {
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port + "\n", e);
		}
	}

	private boolean connectToServer(int server_address, int server_port) {
		logger.info("Connecting to KVserver on port " + server_port + "\n");
		kvServerSocket = new Socket(server_address, server_port);

	}

	private boolean connectToECS(int ecs_addr, int ecs_port) {
		logger.info("Connecting to ECS on port " + server_port + "\n");


	}

	private boolean initializeServer() {
		logger.info("Initialize KVServer ... \n");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("KVServer listening on port: "
					+ serverSocket.getLocalPort() + "\n");
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket: \n");
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound! \n");
			}
			return false;
		}
	}

	private void initializeStorage() {
		try {
			// see if file exists, if not, create it
			File f = new File(storage_file_path);

			if (f.createNewFile()) {
				logger.info("Created storage file at " + storage_file_path + '\n');
			}
		} catch (IOException e) {
			System.out.println("Error! Unable initialize Storage.");
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.ALL);
			if (args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				new KVServer(port, 1, "").start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}
}
