package com.gallop.connect;

import com.gallop.connect.config.DataSoourceProperties;
import com.gallop.connect.logminer.source.LogMinerSession;
import com.gallop.connect.logminer.source.LogMinerSourceConnectorConfig;
import com.gallop.connect.logminer.source.model.LogMinerEvent;
import com.gallop.connect.logminer.source.model.Offset;
import com.gallop.connect.logminer.source.model.Table;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * author gallop
 * date 2021-09-08 10:53
 * Description:
 * Modified By:
 */
public class SessionTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionTest.class);

    private LogMinerSourceConnectorConfig config;
    private LogMinerSession session;

    @Before
    public void initConfig() throws Exception {
        Map<String, String> props = DataSoourceProperties.getProperties();
        config = new LogMinerSourceConnectorConfig(props);
        session = new LogMinerSession(config);
    }

    @After
    public void tearDown() throws Exception {
        if (session != null) {
            session.close();
        }
    }

    //@Test
    public void testLogMinerSessionVisibleTables() throws Exception {
        //Assert.assertTrue(session.isMultitenant());
        List<Table> tables = session.getVisibleTables();
        LOGGER.info("Retrieved list of tables visible in session: {}", tables);

        List<Table> filteredTables = filterTables(tables);
        LOGGER.info("Filtered list of tables: {}", filteredTables);
    }

    private List<Table> filterTables(List<Table> visibleTables) {
        List<String> whitelist = config.getList(LogMinerSourceConnectorConfig.WHITELIST_CONFIG);
        Set<String> whitelistSet = whitelist.isEmpty() ? null : new HashSet<>(whitelist);

        final List<Table> filteredTables = new ArrayList<>(visibleTables.size());
        if (whitelist != null) {
            for (Table table : visibleTables) {
                if (table.matches(whitelistSet)) {
                    filteredTables.add(table);
                }
            }
        }
        return filteredTables;
    }

    //@Test
    public void testMining() throws Exception {
        List<Table> tables = session.getVisibleTables();
        List<Table> filteredTables = filterTables(tables);

        Map<Table, Offset> state = filteredTables.stream()
                .collect(Collectors.toMap(t -> t, t -> Offset.DEFAULT_OFFSET));

        session.start(state);

        while (true) {
            List<LogMinerEvent> events = session.poll(state);
            if (events != null) {
                List<SourceRecord> records = events.stream().map(e -> {
                    SourceRecord r = new SourceRecord(e.getPartition(), e.getOffset(),
                            config.getString(LogMinerSourceConnectorConfig.TOPIC_CONFIG), e.getSchema(), e.getStruct());
                    LOGGER.info("Event to source record-value: {}", r.value());
                    return r;
                }).collect(Collectors.toList());
                LOGGER.info("Cooked up {} records for Kafka", records.size());
            }
            Thread.sleep(3000);
            System.out.println("====================休眠3秒===========================");
        }
    }


}
