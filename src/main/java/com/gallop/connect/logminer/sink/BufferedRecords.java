/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.gallop.connect.logminer.sink;


import com.gallop.connect.logminer.sink.dialect.DatabaseDialect;
import com.gallop.connect.logminer.source.LogMinerSourceConnectorConstants;
import com.gallop.connect.logminer.util.ColumnId;
import com.gallop.connect.logminer.util.TableId;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.gallop.connect.logminer.sink.JdbcSinkConfig.InsertMode.INSERT;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

  private final TableId tableId;
  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final Connection connection;
  private List<SinkRecord> records = new ArrayList<>();
  //private Schema keySchema;
  //private Schema valueSchema;
  //private FieldsMetadata fieldsMetadata;
  private Statement updateStatement;
  private boolean deletesInBatch = false;

  public BufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      Connection connection
  ) {
    this.tableId = tableId;
    this.config = config;
    this.dbDialect = dbDialect;
    //this.dbStructure = dbStructure;
    this.connection = connection;
    //this.recordValidator = RecordValidator.create(config);
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException {
    //recordValidator.validate(record);
    final List<SinkRecord> flushed = new ArrayList<>();
    updateStatement = dbDialect.createStatement(connection);

    if(!isNull(record.value())){
      records.add(record);
    }

    if (records.size() >= config.batchSize) {
      flushed.addAll(flush());
    }
    return flushed;
  }

  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      log.debug("Records is empty");
      return new ArrayList<>();
    }
    log.debug("Flushing {} buffered records", records.size());
    for (SinkRecord record : records) {

      Struct struct = (Struct) record.value();
      String redoSql = struct.getString(LogMinerSourceConnectorConstants.FIELD_SQL_REDO);
      String newRedoSql = checkAndTransformTableSpace(redoSql);
      log.info("---->>>>before execute newRedoSql:"+newRedoSql);
      if (newRedoSql != null && !"".equals(newRedoSql)){
        if(newRedoSql.endsWith(";")){
          newRedoSql = newRedoSql.substring(0,newRedoSql.length()-1);
        }
        this.updateStatement.addBatch(newRedoSql);
      }

    }
    Optional<Long> totalUpdateCount = executeUpdates();
    final long expectedCount = updateRecordCount();
    if (totalUpdateCount.filter(total -> total != expectedCount).isPresent()
        && config.insertMode == INSERT) {
      throw new SQLException(String.format(
          "Update count (%d) did not sum up to total number of records inserted (%d)",
          totalUpdateCount.get(),
          expectedCount
      ));
    }
    if (!totalUpdateCount.isPresent()) {
      log.info(
          "{} records:{} , but no count of the number of rows it affected is available",
          config.insertMode,
          records.size()
      );
    }

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    deletesInBatch = false;
    return flushedRecords;
  }

  /**
   * @return an optional count of all updated rows or an empty optional if no info is available
   */
  private Optional<Long> executeUpdates() throws SQLException {
    Optional<Long> count = Optional.empty();
    //updateStatement.addBatch("select count(*) from test_user");
    for (int updateCount : updateStatement.executeBatch()) {
      if (updateCount != Statement.SUCCESS_NO_INFO) {
        count = count.isPresent()
            ? count.map(total -> total + updateCount)
            : Optional.of((long) updateCount);
      }
    }
    /*if(count.isPresent()){
      System.out.println("----------before return count="+count.get());
    }*/
    return count;
  }

  private long updateRecordCount() {
    return records
        .stream()
        // ignore deletes
        .filter(record -> nonNull(record.value()) || !config.deleteEnabled)
        .count();
  }

  public void close() throws SQLException {
    if (nonNull(updateStatement)) {
      updateStatement.close();
      updateStatement = null;
    }

  }

  private Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
        .map(name -> new ColumnId(tableId, name))
        .collect(Collectors.toList());
  }

  // 正则式获取表空间名字
  private String getTableSpaceName(String redoSql){
    if(!redoSql.contains(".")){
      return null;
    }
    Pattern pattern = Pattern.compile("\\b\\w+\\b(?=\"[.])");// \\b 表示字符串的开头或结尾，这里使用了(?=exp)，也叫零宽度正预测先行断言
    Matcher matcher = pattern.matcher(redoSql);

    if(matcher.find()){
      //System.err.println("result="+matcher.group(0));
      return matcher.group(0);
    }else {
      return null;
    }
  }
  //替换表空间名字
  private String replaceTableSpaceName(String redoSql,String newTableSpaceName){
    if(!redoSql.contains(".")){
      return redoSql;
    }
    Pattern pattern = Pattern.compile("\\b\\w+\\b(?=\"[.])");
    Matcher matcher = pattern.matcher(redoSql);

    StringBuffer sb = new StringBuffer();
    if (matcher.find()) {
      matcher.appendReplacement(sb, newTableSpaceName);
    }
    matcher.appendTail(sb);
    //System.out.println(sb.toString());
    return sb.toString();
  }

  /**
   * @date 2021-09-10 17:04
   * Description: 检查sql语句是否需要处理的范围内，如果是，则检查源表空间和目标表空间是否一样，不一样则替换
   * Param:
   * return:
   **/
  private String checkAndTransformTableSpace(String redoSql){
    String result = null;
    String tableSpaceName = getTableSpaceName(redoSql);
    if(tableSpaceName==null || "".equals(tableSpaceName)){
      return null;
    }else {
      String targetTableSpaceName = config.tableSpaceSourceSinkMap.get(tableSpaceName.toUpperCase());

      if(targetTableSpaceName==null){
        //匹配不到配置文件预设的源数据库表空间，则返回null，即不处理此sql语句
        result = null;
      }else if(!targetTableSpaceName.equalsIgnoreCase(tableSpaceName)){
        //当源数据库的表空间和目标数据库的表空间不一致时，需要替换
        result = replaceTableSpaceName(redoSql,targetTableSpaceName);
      }else {
        result =  redoSql;
      }
    }
    return result;
  }

}
