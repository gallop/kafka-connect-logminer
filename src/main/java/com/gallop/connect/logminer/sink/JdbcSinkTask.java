
package com.gallop.connect.logminer.sink;

import com.gallop.connect.logminer.sink.dialect.DatabaseDialect;
import com.gallop.connect.logminer.sink.dialect.DatabaseDialects;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class JdbcSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(JdbcSinkTask.class);

  ErrantRecordReporter reporter;
  DatabaseDialect dialect;
  JdbcSinkConfig config;
  JdbcDbWriter writer;
  int remainingRetries;
  private OffsetTracker offsetTracker;

  @Override
  public void start(final Map<String, String> props) {
    log.info("Starting JDBC Sink task");
    config = new JdbcSinkConfig(props);
    this.offsetTracker = new OffsetTracker();
    initWriter();
    remainingRetries = config.maxRetries;
    try {
      reporter = context.errantRecordReporter();
    } catch (NoSuchMethodError | NoClassDefFoundError e) {
      // Will occur in Connect runtimes earlier than 2.6
      reporter = null;
    }
  }


  void initWriter() {
    if (config.dialectName != null && !config.dialectName.trim().isEmpty()) {
      dialect = DatabaseDialects.create(config.dialectName, config);
    } else {
      dialect = DatabaseDialects.findBestFor(config.connectionUrl, config);
    }
    log.info("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
    writer = new JdbcDbWriter(config, dialect);
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException{
    if (records.isEmpty()) {
      return;
    }
    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    /*log.debug(
        "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
        + "database...",
        recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
    );*/

    log.info(
            "=====Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
                    + "database...",
            recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
    );

    records.forEach(e->{
      System.out.println(Thread.currentThread().getName()+"-------------topic:"+e.topic()+",records-offset="+e.kafkaOffset()+",kafkaPartition="+e.kafkaPartition());
    });

    try {
      writer.write(records);
    } catch (SQLException sqle) {
      log.warn(
          "Write of {} records failed, remainingRetries={}",
          records.size(),
          remainingRetries,
          sqle
      );
      int totalExceptions = 0;
      for (Throwable e :sqle) {
        totalExceptions++;
      }
      writer.closeQuietly();
      initWriter();

      if (reporter != null) {
        //批量sql处理失败后，转成单条sql执行，失败的直接进死信队列。
        unrollAndRetry(records);
      } else {
        SQLException sqlAllMessagesException = getAllMessagesException(sqle);
        log.error(
                "Failing task after exhausting retries; "
                        + "encountered {} exceptions on last write attempt. "
                        + "For complete details on each exception, please enable DEBUG logging.",
                totalExceptions);
        int exceptionCount = 1;
        for (Throwable e : sqle) {
          log.debug("Exception {}:", exceptionCount++, e);
        }
        throw new ConnectException(sqlAllMessagesException);
      }
      /*if (remainingRetries > 0) {
        writer.closeQuietly();
        initWriter();
        remainingRetries--;
        context.timeout(config.retryBackoffMs);
        // 抛出此异常后，kafka client api 会重新执行调用put的方法，也就是此方法在执行一次
        throw new RetriableException(sqlAllMessagesException);
      } else if(remainingRetries==0){
        remainingRetries--;
        return;
      }else {
        System.out.println("-------->>reporter="+reporter);
        if (reporter != null) {
          System.out.println("---------------reporter==== is not null!!!!!!!!");
          unrollAndRetry(records);
        } else {
          log.error(
              "Failing task after exhausting retries; "
                  + "encountered {} exceptions on last write attempt. "
                  + "For complete details on each exception, please enable DEBUG logging.",
              totalExceptions);
          int exceptionCount = 1;
          for (Throwable e : sqle) {
            log.debug("Exception {}:", exceptionCount++, e);
          }
          throw new ConnectException(sqlAllMessagesException);
        }
      }
      remainingRetries = config.maxRetries;*/
    }

  }

  private void unrollAndRetry(Collection<SinkRecord> records) {
    writer.closeQuietly();
    //offsetTracker.setExceptionFlag(true);
    for (SinkRecord record : records) {
      //OffsetTracker.OffsetState offsetState = offsetTracker.addPendingRecord(record);
      try {
        writer.write(Collections.singletonList(record));
      } catch (SQLException sqle) {
        SQLException sqlAllMessagesException = getAllMessagesException(sqle);
        reporter.report(record, sqlAllMessagesException);
        writer.closeQuietly();
      }
      //offsetState.markProcessed();
      //offsetTracker.updateOffsets();
    }
  }

  private SQLException getAllMessagesException(SQLException sqle) {
    String sqleAllMessages = "Exception chain:" + System.lineSeparator();
    for (Throwable e : sqle) {
      sqleAllMessages += e + System.lineSeparator();
    }
    SQLException sqlAllMessagesException = new SQLException(sqleAllMessages);
    sqlAllMessagesException.setNextException(sqle);
    return sqlAllMessagesException;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // Not necessary
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {

    /*if(offsetTracker.isExceptionFlag()){
      offsetTracker.setExceptionFlag(false);
      Map<TopicPartition, OffsetAndMetadata> map = offsetTracker.offsets();
      map.forEach((key,value)->{
        System.out.println(Thread.currentThread().getName()+"-->>>>>>>>>>>>>>>>>>>>>>>>>>preCommit---TopicPartition:"+key.topic());
        System.out.println(Thread.currentThread().getName()+"-->>>>>>>>>>>>>>>>>>>>>>>>>>preCommit---offset:"+value.offset());
      });

      //context.offset(offsetTracker.getMaxOffsetByPartition());
      return offsetTracker.offsets();
    }*/
    return super.preCommit(currentOffsets);
  }

  public void stop() {
    log.info("Stopping task");
    try {
      writer.closeQuietly();
    } finally {
      try {
        if (dialect != null) {
          dialect.close();
        }
      } catch (Throwable t) {
        log.warn("Error while closing the {} dialect: ", dialect.name(), t);
      } finally {
        dialect = null;
      }
    }
  }

  @Override
  public String version() {
    return "1.0.0";
  }




}
