package co.elastic.apm.agent.plugins.s3.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * This is a server which just accepts lines of JSON code and if the JSON
 * is valid and the root node is "span", then adds that JSON object
 * to a span list which is accessible externally to the class.
 * <p>
 * The Elastic agent sends lines of JSON code, and so this mock server
 * can be used as a basic APM server for testing.
 * <p>
 * The HTTP server used is the JDK embedded com.sun.net.httpserver
 */
public class MockApmServer {
    /**
     * Simple main that starts a mock APM server, prints the port it is
     * running on, and exits after 2_000 seconds. This is not needed
     * for testing, it is just a convenient template for trying things out
     * if you want play around.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        MockApmServer server = new MockApmServer();
        System.out.println(server.start());
        server.blockUntilReady();
        Thread.sleep(2_000_000L);
        server.stop();
        server.blockUntilStopped();
    }

    private static volatile HttpServer TheServerInstance;

    private final List<JsonNode> spans = new ArrayList<>();

    /**
     * A count of the number of spans received and not yet removed
     *
     * @return the number of spans received and not yet removed
     */
    public int getSpanCount() {
        synchronized (spans) {
            return spans.size();
        }
    }

    /**
     * Gets the span at index i if it exists within the timeout
     * specified, and removes it from the span list.
     * If it doesn't exist within the timeout period, an
     * IllegalArgumentException is thrown
     *
     * @param i               - the index to retrieve a span from
     * @param timeOutInMillis - millisecond timeout to wait for the
     *                        span at index i to exist
     * @return - the span information as a JSON object
     * @throws TimeoutException - thrown if no span
     *                          exists at index i by timeout
     */
    public JsonNode getAndRemoveSpan(int i, long timeOutInMillis) throws TimeoutException {
        //because the agent writes to the server asynchronously,
        //any span created in a client is not here immediately
        long start = System.currentTimeMillis();
        long elapsedTime = 0;
        while (elapsedTime < timeOutInMillis) {
            synchronized (spans) {
                if (spans.size() > i) {
                    break;
                }
                if (timeOutInMillis - elapsedTime > 0) {
                    try {
                        spans.wait(timeOutInMillis - elapsedTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                elapsedTime = System.currentTimeMillis() - start;
            }
        }
        synchronized (spans) {
            if (spans.size() <= i) {
                throw new TimeoutException("The apm server does not have a span at index " + i);
            }
        }
        synchronized (spans) {
            return spans.remove(i);
        }
    }

    /**
     * Start the Mock APM server. Just returns empty JSON structures for every incoming message
     *
     * @return - the port the Mock APM server started on
     * @throws IOException
     */
    public synchronized int start() throws IOException {
        if (TheServerInstance != null) {
            throw new IOException("MockApmServer: Ooops, you can't start this instance more than once");
        }
        InetSocketAddress addr = new InetSocketAddress("0.0.0.0", 0);
        HttpServer server = HttpServer.create(addr, 10);
        server.createContext("/exit", new ExitHandler());
        server.createContext("/", new RootHandler());

        server.start();
        TheServerInstance = server;
        System.out.println("MockApmServer started on port " + server.getAddress().getPort());
        return server.getAddress().getPort();
    }

    /**
     * Stop the server gracefully if possible
     */
    public synchronized void stop() {
        TheServerInstance.stop(1);
        TheServerInstance = null;
    }

    class RootHandler implements HttpHandler {
        public void handle(HttpExchange t) {
            try {
                InputStream body = t.getRequestBody();
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                byte[] buffer = new byte[8 * 1024];
                int lengthRead;
                while ((lengthRead = body.read(buffer)) > 0) {
                    bytes.write(buffer, 0, lengthRead);
                }
                reportspansAndMetrics(bytes.toString());
                String response = "{}";
                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void reportspansAndMetrics(String json) {
            String[] lines = json.split("[\r\n]");
            for (String line : lines) {
                reportspanOrMetric(line);
            }
        }

        private void reportspanOrMetric(String line) {
            //System.out.println("MockApmServer reading JSON objects: "+ line);
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode messageRootNode = null;
            try {
                messageRootNode = objectMapper.readTree(line);
                JsonNode spanNode = messageRootNode.get("span");
                if (spanNode != null) {
                    synchronized (spans) {
                        spans.add(spanNode);
                        spans.notify();
                    }
                }
            } catch (JsonProcessingException e) {
                System.out.println("Not JSON: " + line);
                e.printStackTrace();
            }
        }
    }

    static class ExitHandler implements HttpHandler {
        private static final int STOP_TIME = 3;

        public void handle(HttpExchange t) {
            try {
                InputStream body = t.getRequestBody();
                String response = "{}";
                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();
                TheServerInstance.stop(STOP_TIME);
                TheServerInstance = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Wait until the server is ready to accept messages
     */
    public void blockUntilReady() {
        while (TheServerInstance == null) {
            try {
                Thread.sleep(1L);
            } catch (InterruptedException e) {
                // do nothing, just enter the next sleep
            }
        }
    }

    /**
     * Wait until the server is terminated
     */
    public void blockUntilStopped() {
        while (TheServerInstance != null) {
            try {
                Thread.sleep(1L);
            } catch (InterruptedException e) {
                // do nothing, just enter the next sleep
            }
        }
    }
}