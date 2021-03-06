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

package com.gallop.connect.logminer;

import com.gallop.connect.logminer.source.*;
import com.gallop.connect.logminer.source.model.Table;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class LogMinerSourceConnector extends SourceConnector {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerSourceConnector.class);

	private Map<String, String> configProperties;
	private LogMinerSourceConnectorConfig config;
	private DictionaryMonitorThread tableMonitorThread;

	@Override
	public String version() {
		return "1.0.0";
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("==========="+Thread.currentThread().getName()+"---Starting LogMiner source connector");
		try {
			configProperties = props;
			config = new LogMinerSourceConnectorConfig(props);
		} catch (ConfigException e) {
			throw new ConnectException("Cannot start connector, configuration error", e);
		}

		// TODO: add config options for hysteretic poll interval
		long tablePollInterval = config.getLong(LogMinerSourceConnectorConfig.TABLE_POLL_INTERVAL_CONFIG);
		List<String> whitelist = config.getList(LogMinerSourceConnectorConfig.WHITELIST_CONFIG);
		Set<String> whitelistSet = whitelist.isEmpty() ? null : new HashSet<>(whitelist);
		List<String> blacklist = config.getList(LogMinerSourceConnectorConfig.BLACKLIST_CONFIG);
		Set<String> blacklistSet = blacklist.isEmpty() ? null : new HashSet<>(blacklist);

		LogMinerSession session = null;
		try {
			if (whitelistSet != null && blacklistSet != null) {
				throw new ConnectException(LogMinerSourceConnectorConfig.WHITELIST_CONFIG + " and "
						+ LogMinerSourceConnectorConfig.BLACKLIST_CONFIG + " are " + "exclusive.");
			}
			session = new LogMinerSession(config);
			tableMonitorThread = new DictionaryMonitorThread(session, context, tablePollInterval, whitelistSet,
					blacklistSet);
			tableMonitorThread.start();
		} catch (Exception e) {
			if (session != null) {
				session.close();
			}
		}
	}

	@Override
	public Class<? extends Task> taskClass() {
		return LogMinerSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Table> tables = tableMonitorThread.tables();
		int numGroups = Math.min(tables.size(), maxTasks);
		List<List<Table>> tableGroups = groupTables(tables, numGroups);

		List<Map<String, String>> taskConfigs = new ArrayList<>(tableGroups.size());
		for (List<Table> tableGroup : tableGroups) {
			Map<String, String> taskProps = new HashMap<>(configProperties);
			String tablesConfig = tableGroup.stream().map(Table::getQName).collect(Collectors.joining(","));
			taskProps.put(LogMinerSourceTaskConfig.TABLES_CONFIG, tablesConfig);
			taskConfigs.add(taskProps);
		}

		LOGGER.debug("{} tasks configured for {} tables", tableGroups.size(), tables.size());
		return taskConfigs;
	}

	@Override
	public void stop() {
		LOGGER.info("Stopping LogMiner source connector");
	}

	@Override
	public ConfigDef config() {
		return LogMinerSourceConnectorConfig.CONFIG_DEF;
	}

	/**
	 * Given a list of @see Comparable elements and a target number of groups,
	 * generates list of groups of elements to match the target number of groups,
	 * spreading them "evenly" among the groups. This generates groups with elements
	 * round-robin distributed according to their natural order, e.g. a performance
	 * related statistic.
	 *
	 * @param elements  list of elements to partition
	 * @param groups the number of output groups to generate.
	 */
	public static <T extends Comparable<T>> List<List<T>> groupTables(List<T> elements, int groups) {
		List<T> sortedElements = new ArrayList<T>(elements);
		Collections.sort(sortedElements);

		List<List<T>> result = new ArrayList<>(groups);
		for (int i = 0; i < groups; i++)
			result.add(new ArrayList<>());

		Iterator<T> iterator = sortedElements.iterator();
		for (int i = 0; iterator.hasNext(); i++)
			result.get(i % groups).add(iterator.next());

		return result;
	}
}
