package listeners;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinLogEventListener implements BinaryLogClient.EventListener {
    private static final Logger log = LoggerFactory.getLogger(BinLogEventListener.class);
    @Override
    public void onEvent(Event event) {
        log.info("Event type", ((EventHeaderV4)event.getHeader()).getEventType());
    }
}
