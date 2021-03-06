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

package com.gallop.connect.logminer.source.dialect;

import com.gallop.connect.logminer.source.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleInstanceLogMinerDialect extends BaseLogMinerDialect {
	private static final Logger LOGGER = LoggerFactory.getLogger(SingleInstanceLogMinerDialect.class);

	private static final String PROPERTIES_FILE = "/sql-single.properties";

	private static Map<Statement, String> STATEMENTS;

	static {
		STATEMENTS = new HashMap<Statement, String>();
		try {
			initializeStatements(STATEMENTS, PROPERTIES_FILE);
		} catch (Exception e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	@Override
	public String getStatement(Statement statement) {
		if (STATEMENTS.containsKey(statement)) {
			return STATEMENTS.get(statement);
		}
		return super.getStatement(statement);
	}

	@Override
	public List<Table> getTables(Connection connection) throws SQLException {
		List<Table> tables = new ArrayList<Table>();
		String query = getStatement(Statement.TABLES);
		try (PreparedStatement p = connection.prepareStatement(query)) {
			ResultSet rs = p.executeQuery();
			while (rs.next()) {
				tables.add(new Table(rs.getString(1), rs.getString(2), rs.getString(3)));
			}
		}
		return tables;
	}

}
