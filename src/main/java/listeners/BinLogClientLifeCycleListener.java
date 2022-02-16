package listeners;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinLogClientLifeCycleListener implements BinaryLogClient.LifecycleListener {
    private static final Logger log = LoggerFactory.getLogger(BinLogClientLifeCycleListener.class);
    private boolean disconnected = true;

    @Override
    public void onConnect(BinaryLogClient binaryLogClient) {

    }

    @Override
    public void onCommunicationFailure(BinaryLogClient binaryLogClient, Exception e) {

    }

    @Override
    public void onEventDeserializationFailure(BinaryLogClient binaryLogClient, Exception e) {

    }

    @Override
    public void onDisconnect(BinaryLogClient binaryLogClient) {

    }
}
