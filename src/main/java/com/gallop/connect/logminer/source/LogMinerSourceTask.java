/**
 * Copyright 2018 David Arnold
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.gallop.connect.logminer.source;

import com.gallop.connect.logminer.source.model.LogMinerEvent;
import com.gallop.connect.logminer.source.model.Offset;
import com.gallop.connect.logminer.source.model.Table;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Every instance manages a set of [pluggable.]owner.table partitions
 * 
 * @author David Arnold
 *
 */
public class LogMinerSourceTask extends SourceTask {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerSourceTask.class);

	private LogMinerSourceTaskConfig config;
	private LogMinerSession session;
	private List<Map<String, String>> partitions;
	private final AtomicBoolean running = new AtomicBoolean(false);
	private final Map<Table, Offset> state = new HashMap<>();

	public LogMinerSourceTask() {
	}

	@Override
	public String version() {
		return "1.0.0";
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("-------Thread:"+Thread.currentThread().getName()+"--Starting LogMinerSourceTask instance");
		config = new LogMinerSourceTaskConfig(props);

		try {
			session = new LogMinerSession(config);

			List<String> tables = config.getList(LogMinerSourceTaskConfig.TABLES_CONFIG);
			LOGGER.debug("-->--->--->--->Configured task tables: {}", tables);
			this.partitions = new ArrayList<>(tables.size());
			for (String table : tables) {
				Map<String, String> tablePartition = Collections
						.singletonMap(LogMinerSourceConnectorConstants.TABLE_NAME_KEY, table+LogMinerSourceConnectorConstants.EVENT_SCHEMA_QUALIFIER);
				partitions.add(tablePartition);
			}


			session.start(getCurrentState());
			running.set(true);
			LOGGER.info("Started LogMinerSourceTask");
		} catch (Exception e) {
			throw new ConnectException("Cannot start LogMinerSourceTask instance", e);
		}
	}

	private Map<Table, Offset> getCurrentState(){
		LOGGER.debug("--<---<---<--->Requesting offsets for partitions: {}", partitions);
		//这里获取kafka connect 帮我们管理的offset
		Map<Map<String, String>, Map<String, Object>> offsets = context.offsetStorageReader().offsets(this.partitions);
		LOGGER.debug("-->--->--->--->Returned offsets: {}", offsets);
		// {{Table=ORCL.USERCENTER.test_user.EVENT}=null}

		//Map<Table, Offset> state = new HashMap<>();
		if (offsets != null) {
			for (Map<String, String> partition : offsets.keySet()) {
				LOGGER.info("Setting offset for returned partition {}", partition);
				Table t = Table.fromQName(partition.get(LogMinerSourceConnectorConstants.TABLE_NAME_KEY));
				Offset o = Offset.fromMap(offsets.get(partition));
				state.put(t, o);
			}
		}
		/*else {
			for (Map<String, String> partition : offsets.keySet()) {
				Table t = Table.fromQName(partition.get(LogMinerSourceConnectorConstants.TABLE_NAME_KEY));
				state.put(t, Offset.DEFAULT_OFFSET);
			}
		}*/
		return state;
	}

	private void updateCurrentState(Map<String, String> partition,Offset offset){
		Table t = Table.fromQName(partition.get(LogMinerSourceConnectorConstants.TABLE_NAME_KEY));
		state.put(t, offset);
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		LOGGER.debug("---------"+Thread.currentThread().getName()+"--Polling for new events");
		TimeUnit.MILLISECONDS.sleep(600);
		try {
			List<LogMinerEvent> events = session.poll(this.state);
			if (events == null || events.size()==0)
				return null;
			LOGGER.info("---------"+Thread.currentThread().getName()+"--Polling for new events-size="+events.size());
			// TODO: either consider implementing topic prefix of some sort, or remove
			// config option in favour of one topic partitioned by table
			return events.stream()
					.map(e -> {
						updateCurrentState(e.getPartition(),e.getOffsetObject());
						return new SourceRecord(e.getPartition(), e.getOffset(),
							config.getString(LogMinerSourceTaskConfig.TOPIC_CONFIG), e.getSchema(), e.getStruct());
					}).collect(Collectors.toList());
		} catch (Exception e) {
			throw new ConnectException("Error during LogMinerSourceTask poll", e);
		}
	}

	@Override
	public void stop() {
		LOGGER.info("Stopping LogMiner source task");
		if(session !=null){
			session.close();
		}
		running.set(false);
	}
}
