package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import server.KVClientConnection;
import shared.messages.IKVMessage.StatusType;

public class KVServer extends Thread implements IKVServer {

	private static Logger logger = Logger.getRootLogger();

	private int port;
	private ServerSocket serverSocket;
	private boolean running;
	private int cacheSize;
    private CacheStrategy strategy;

	private KVDB db;
	private KVCache cache;

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
		this.strategy = CacheStrategy.valueOf(strategy);

		initializeServer();
	}

	@Override
	public int getPort() {
		return this.port;
		// try just port?
	}

	@Override
	public String getHostname() {
		// ---------- TEST ----------
		try {
            InetAddress sv = InetAddress.getLocalHost();

            return sv.getHostName();
        } catch (UnknownHostException ex) {
            logger.error("Error! Unknown Host. \n", ex);
			
            return null;
        }
	}

	@Override
	public CacheStrategy getCacheStrategy() {
		// ---------- TEST ----------
		return this.strategy;
		// try just strategy?
	}

	@Override
	public int getCacheSize() {
		// ---------- TEST ----------
		return this.cacheSize;
		// try just cacheSize?
	}

	@Override
	public boolean inStorage(String key) {
		// ---------- TEST ----------
		try {
            boolean exists = (db.getKV(key) != null);

            if (exists) {
				logger.info("Key :: " + key + " found in storage. \n");
			} else {
				logger.info("Key :: " + key + " not found in storage. \n");
			}

            return exists;

        } catch (IOException e) {
            logger.error("IO Failure. \n", e);

            return false;
        }
	}

	@Override
	public boolean inCache(String key) {
		// ---------- TEST ----------
		if (this.strategy == CacheStrategy.None) {
			return false;
		}

        boolean exists = (cache.getKV(key) != null);

        if (exists) { 
			logger.info("Key :: " + key + " found in cache. \n");
		} else {
			logger.info("Key :: " + key + " not found in cache. \n");
		}

        return exists;
	}

	@Override
	public String getKV(String key) throws Exception {
		// ---------- TEST ----------
		// add db to get from storage?
		try {
			if (getCacheStrategy() != CacheStrategy.None) {

				String val = cache.getKV(key);
	
				if (val != null) {
					logger.info("Key :: " + key + ", Value :: " + val + "\n");
	
					return val;
				} else {
					logger.info("Key :: " + key + " has no associated value. \n");

					return null;
				}
			}
		} catch (IOException e) {
			logger.error("IO Failure. \n", e);
			
			return null;
		}
	}

	@Override
	public StatusType putKV(String key, String value) throws Exception {
		// ---------- TEST ----------
		// add db to put into storage?
		try {
			if (getCacheStrategy() != CacheStrategy.None) {
				// check if cache already contains key-value pair
				String test = cache.getKV(key, value);

				if (test != null) {
					if (value == null) {
						logger.info("Deleting key-value pair from cache: Key :: " + key + ", Value :: " + value + "\n");
						// IDK HOW TO DELETE AH
					} else {
						logger.info("Overwriting key-value pair into cache: Key :: " + key + ", Value :: " + value + "\n");
						cache.putKV(key, value);
						// MAYBE DELETE FIRST THEN PUT AGAIN?
					}
				} else {
					if (value == null) {
						logger.info("Error: delete failed - key-value pair does not exist in cache \n");
						return null;
						// return null since it failed
					} else {
						logger.info("Put into cache: Key :: " + key + ", Value :: " + value);
						cache.putKV(key, value);
						// put into cache on success
					}
				}
			}
		} catch (IOException e) {
			logger.error("IO Failure. \n", e);
			
			return null;
		}
	}

	@Override
	public void clearCache() {
		// ---------- TEST ----------
		logger.info("Clear Cache. \n");
        if (this.strategy != CacheStrategy.None) {
            cache.clear();
		}
	}

	@Override
	public void clearStorage() {
		// ---------- TEST ----------
		try {
            clearCache();
            logger.info("Clear Storage. \n");
            db.clear();
        } catch (IOException e) {
            logger.error("Cannot clear Storage. ");
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
		// TODO Auto-generated method stub
	}

	private boolean isRunning() {
		return this.running;
		// try just running?
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

	private boolean initializeServer() {
		logger.info("Initialize server ... \n");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
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
