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

	private KVDB storage;
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
	}

	@Override
	public String getHostname() {
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
		return this.strategy;
	}

	@Override
	public int getCacheSize() {
		return this.cacheSize;
	}

	@Override
	public boolean inStorage(String key) {
		try {
            boolean exists = (storage.getKV(key) != null);

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
		try {
			if (getCacheStrategy() != CacheStrategy.None) {

				String val = cache.getKV(key);
	
				if (val != null) {
					logger.info("GET_SUCCESS " + key + val + "\n");
	
					return val;
				} else {
					logger.info("GET_ERROR " + key + "\n");

					return null;
				}
			}

			// Reaching this point means the key was not found in cache
			// Deep storage needs to be checked
			String val_2 = storage.getKV(key);

			if (getCacheStrategy() != CacheStrategy.None && val_2 != null) {
				// Insert found value into the cache
				cache.putKV(key, value);

				logger.info("Key :: " + key + ", Value :: " + val_2 + "\n");

				return val_2;
			} else {
				logger.info("Key :: " + key + " has no associated value. \n");

				return null;
			}
		} catch (IOException e) {
			logger.error("IO Failure. \n", e);
			
			return null;
		}
	}

	@Override
	public StatusType putKV(String key, String value) throws Exception {
		try {
			if (getCacheStrategy() != CacheStrategy.None) {
				// check if cache already contains key-value pair
				String test = cache.getKV(key, value);

				if (test != null) {
					if (value == null) {
						Boolean status = cache.delete(key);

						if (status) {
							logger.info("Deleting key-value pair from cache: Key :: " + key + ", Value :: " + value + "\n");
						} else {
							logger.info("FAILED " + "cache deletion of " + key + ", " + value + "\n");
						}
					} else {
						cache.putKV(key, value);
						storage.putKV(key, value);

						logger.info("PUT_UPDATE " + key + value + "\n");
					}
				} else {
					if (value == null) {
						logger.info("PUT_ERROR " + key + "\n");
						return null;
						// return null since it failed
					} else {
						cache.putKV(key, value);
						storage.putKV(key, value);

						logger.info("PUT_SUCCESS " + key + value + "\n");
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
		logger.info("Clear Cache. \n");
        if (this.strategy != CacheStrategy.None) {
            cache.clear();
		}
	}

	@Override
	public void clearStorage() {
		try {
            clearCache();
            logger.info("Clear Storage. \n");
            storage.clear();
        } catch (IOException e) {
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
            client.stop();
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! Termination failure on port: " + port, e);
        }
	}

	private boolean isRunning() {
		return this.running;
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
