import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import listeners.BinLogClientLifeCycleListener;
import listeners.BinLogEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BinLogReader {

    private static Logger log = LoggerFactory.getLogger(BinLogReader.class);
    private BinaryLogClient binaryLogClient;
    private String hostname;
    private int port;
    private String username;
    private String password;
    private BinLogEventListener binLogEventListener;
    private BinLogClientLifeCycleListener binLogClientLifeCycleListener;

    public static void main(String args[]) {
        log.debug("Binlog reader Hello World !");
    }

    public BinLogReader(String hostname, int port, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    private BinaryLogClient initialize() {
        if (binaryLogClient == null) {
            binaryLogClient = new BinaryLogClient(hostname, port, username, password);
            binLogClientLifeCycleListener = new BinLogClientLifeCycleListener();
            binLogEventListener = new BinLogEventListener();
            binaryLogClient.registerLifecycleListener(binLogClientLifeCycleListener);
            EventDeserializer eventDeserializer = new EventDeserializer();
            binaryLogClient.setEventDeserializer(eventDeserializer);
            binaryLogClient.registerEventListener(binLogEventListener);
        }
        return binaryLogClient;
    }

    public void start() throws IOException {
        if (binaryLogClient == null) {
            initialize();
        }
        binaryLogClient.connect();
    }

    public void stop() throws IOException {
        if (binaryLogClient != null &&  binaryLogClient.isConnected()) {
            binaryLogClient.unregisterEventListener(binLogEventListener);
            binaryLogClient.unregisterLifecycleListener(binLogClientLifeCycleListener);
            binaryLogClient.disconnect();
        }
    }
}
