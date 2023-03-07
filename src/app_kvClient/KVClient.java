package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.Thread.State;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;
import client.KVStore;
import client.ClientSocketListener;

public class KVClient implements IKVClient, ClientSocketListener {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;

    private KVStore kvStore = null;
    private boolean stop = false;
    private boolean connected = false;

    private String serverAddress;
    private int serverPort;

    public void run() {
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")) {
            if (tokens.length == 3) {
                try {
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                } catch (NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } catch (IOException e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
                } catch (Exception e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("put")) {
            if (kvStore != null && kvStore.isRunning()) {
                if (tokens.length == 3) {
                    try {
                        connectToResponsibleServer(tokens[1]);
                        kvStore.put(tokens[1], tokens[2]);
                    } catch (Exception e) {
                        printError("Unable to send message!");
                        disconnect();
                    }
                } else if (tokens.length == 2) {
                    try {
                        connectToResponsibleServer(tokens[1]);
                        kvStore.put(tokens[1], null); // delete operation
                    } catch (Exception e) {
                        printError("Unable to send message!");
                        disconnect();
                    }
                } else {
                    printError("Incorrect format: expecting put <key> <value>");
                }
            } else {
                printError("Not connected!");
            }

        } else if (tokens[0].equals("get")) {
            if (kvStore != null && kvStore.isRunning()) {
                if (tokens.length == 2) {
                    try {
                        connectToResponsibleServer(tokens[1]);
                        kvStore.get(tokens[1]);
                    } catch (Exception e) {

                        disconnect();
                    }
                } else {
                    printError("Incorrect format: expecting get <key>");
                }
            } else {
                printError("Not connected!");
            }

        } else if (tokens[0].equals("keyrange")) {
            if (kvStore != null && kvStore.isRunning()) {
                if (tokens.length == 1) {
                    try {
                        kvStore.keyrange();
                    } catch (Exception e) {

                        disconnect();
                    }
                } else {
                    printError("Incorrect format: expecting keyrange");
                }
            } else {
                printError("Not connected!");
            }
        } else if (tokens[0].equals("disconnect")) {
            disconnect();

        } else if (tokens[0].equals("logLevel")) {
            if (tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void disconnect() {
        if (kvStore != null && kvStore.isRunning() == true) {
            kvStore.disconnect();
            kvStore = null;
        }
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception, UnknownHostException, IOException {
        kvStore = new KVStore(hostname, port);
        kvStore.connect();
        kvStore.addListener(this);
        kvStore.start();
    }

    @Override
    public KVStore getStore() {
        return kvStore;
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("send <text message>");
        sb.append("\t\t sends a text message to the server \n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private String setLevel(String levelString) {

        if (levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if (levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if (levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if (levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if (levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void connectToResponsibleServer(String key) {
        String respServer = kvStore.getResponsible(key);
        if (!respServer.equals(serverAddress + ":" + serverPort)) {
            disconnect();
            String[] respServer_split = respServer.split(":");
            try {
                this.serverAddress = respServer_split[0].trim();
                this.serverPort = Integer.parseInt(respServer_split[1]);
                newConnection(serverAddress, serverPort);
                // wait for input / output streams to be initialized
                while (!kvStore.areStreamsOpen()) {
                    Thread.sleep(50);
                }
            } catch (NumberFormatException nfe) {
                printError("No valid address. Port must be a number!");
                logger.info("Unable to parse argument <port>", nfe);
            } catch (UnknownHostException e) {
                printError("Unknown Host!");
                logger.info("Unknown Host!", e);
            } catch (IOException e) {
                printError("Could not establish connection!");
                logger.warn("Could not establish connection!", e);
            } catch (Exception e) {
                printError("Could not establish connection!");
                logger.warn("Could not establish connection!", e);
            }
        }
    }
    private void retryOperation(KVMessage msg) {
        try {
            // retry operations
            if (msg.getStatus() == StatusType.GET) {
                kvStore.get(msg.getKey());
            } else if (msg.getStatus() == StatusType.PUT) {
                kvStore.put(msg.getKey(), msg.getValue());
            }
        } catch (UnknownHostException e) {
            printError("Unknown Host!");
            logger.info("Unknown Host!", e);
        } catch (IOException e) {
            printError("Could not establish connection!");
            logger.warn("Could not establish connection!", e);
        } catch (Exception e) {
            printError("Error with get or put in retryOperation!");
            e.printStackTrace();
        }
    }

    @Override
    public void handleNewMessage(KVMessage msg) {
        if (!stop) {
            StatusType status = msg.getStatus();
            if (status == StatusType.STRING) {
                System.out.println(msg.getKey());
            } else if (status == StatusType.METADATA) {
                logger.debug("new metadata is " + msg.getValueAsMetadata().toString());
                kvStore.setMetaData(msg.getValueAsMetadata());
            } else if (status == StatusType.PUT || status == StatusType.GET) {
                try {
                    connectToResponsibleServer(msg.getKey());
                    retryOperation(msg);
                } catch (Exception e) {
                    logger.error("Error when retrying the the command to the responsible server!");
                }
            } else if (status == StatusType.KEYRANGE_SUCCESS) {
                System.out.println(msg.getValue());
            } else if (status == StatusType.PUT_SUCCESS) {
                System.out.println("PUT SUCCESS");
            } else if (status == StatusType.PUT_UPDATE) {
                System.out.println("PUT UPDATE");
            } else if (status == StatusType.PUT_ERROR) {
                System.out.println("PUT ERROR");
            } else if (status == StatusType.GET_SUCCESS) {
                System.out.println(msg.getValue());
            } else if (status == StatusType.GET_ERROR) {
                System.out.println("Could not find value for the given key");
            } else if (status == StatusType.DELETE_SUCCESS) {
                System.out.println("DELETE SUCCESS");
            } else if (status == StatusType.DELETE_ERROR) {
                System.out.println("DELETE ERROR");
            }
            System.out.print(PROMPT);
        }
    }

    @Override
    public void handleStatus(SocketStatus status) {
        if (status == SocketStatus.CONNECTED) {

        } else if (status == SocketStatus.DISCONNECTED) {
            System.out.print(PROMPT);
            System.out.println("Connection terminated: "
                    + serverAddress + " / " + serverPort);

        } else if (status == SocketStatus.CONNECTION_LOST) {
            System.out.println("Connection lost: "
                    + serverAddress + " / " + serverPort);
            System.out.print(PROMPT);
        }

    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    /**
     * Main entry point for the kvclient application.
     * 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            KVClient kvclient = new KVClient();
            kvclient.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
