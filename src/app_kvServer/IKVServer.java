package app_kvServer;

import shared.messages.IKVMessage.StatusType;

public interface IKVServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };

    /**
     * Get the port number of the server
     * 
     * @return port number
     */
    public int getPort();

    /**
     * Get the cache strategy of the server
     * 
     * @return cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     * 
     * @return cache size
     */
    public int getCacheSize();


    /**
     * Get the value associated with the key
     * 
     * @return value associated with key
     * @throws Exception
     *                   when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * 
     * @throws Exception
     *                   when key not in the key range of the server
     */
    public StatusType putKV(String key, String value) throws Exception;

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();
}
