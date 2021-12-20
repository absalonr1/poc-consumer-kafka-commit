package kafkaretry;

import java.io.IOException;
import java.net.InetSocketAddress;
import com.sun.net.httpserver.HttpServer;
import org.apache.log4j.Logger;

class Health {
  private static final Logger log = Logger.getLogger(SimpleConsumer.class); 
  private static final int OK = 200;
  private static final int ERROR = 500;

  private final String path;
  private HttpServer server;

  Health(String path) {
    this.path = path;
  }

  void start() {
    try {
      log.info("Starting web server. Env:"+System.getenv("MY_POD_IP"));
      server = HttpServer.create(new InetSocketAddress(System.getenv("MY_POD_IP"), 8080), 0);
    } catch (IOException ioe) {
        log.error(ioe.getStackTrace());
    }
    server.createContext(path, exchange -> {
      log.info(path);
      int responseCode = OK; //streams.state().isRunning() ? OK : ERROR;
      exchange.sendResponseHeaders(responseCode, 0);
      exchange.close();
    });
    log.info("Listening /health...");
    server.start();
  }

  void stop() {
    server.stop(0);
  }

}