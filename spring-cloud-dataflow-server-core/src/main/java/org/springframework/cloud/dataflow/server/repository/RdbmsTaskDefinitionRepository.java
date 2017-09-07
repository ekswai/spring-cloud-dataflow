/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.dataflow.server.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.sql.DataSource;

import javafx.concurrent.Task;
import org.springframework.cloud.dataflow.core.TaskDefinition;
import org.springframework.cloud.dataflow.server.repository.support.SearchPageable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.Assert;

/**
 * RDBMS implementation of {@link TaskDefinitionRepository}.
 *
 * @author Glenn Renfro
 * @author Ilayaperumal Gopinathan
 */
public class RdbmsTaskDefinitionRepository extends AbstractRdbmsKeyValueRepository<TaskDefinition>
		implements TaskDefinitionRepository {

	public RdbmsTaskDefinitionRepository(DataSource dataSource) {
		super(dataSource, "TASK_", "DEFINITIONS", new RowMapper<TaskDefinition>() {
			@Override
			public TaskDefinition mapRow(ResultSet resultSet, int i) throws SQLException {
				return new TaskDefinition(resultSet.getString("DEFINITION_NAME"),
                                          resultSet.getString("DEFINITION"));
			}
		}, "DEFINITION_NAME", "DEFINITION");
        saveRow = "INSERT into " + tableName + "(DEFINITION_NAME, DEFINITION, CREATOR)" + "values (?, ?, ?)";
	}

	@Override
	public TaskDefinition save(TaskDefinition definition) {
		Assert.notNull(definition, "definition must not be null");
		if (exists(definition.getName())) {
			throw new DuplicateTaskException(String.format(
					"Cannot register task %s because another one has already " + "been registered with the same name",
					definition.getName()));
		}
        final String creator = SecurityContextHolder.getContext().getAuthentication().getName();
		Object[] insertParameters = new Object[] { definition.getName(), definition.getDslText(), creator };
		jdbcTemplate.update(saveRow, insertParameters, new int[] { Types.VARCHAR, Types.CLOB, Types.VARCHAR });
		return definition;
	}

	@Override
	public void delete(TaskDefinition definition) {
		Assert.notNull(definition, "definition must not null");
		delete(definition.getName());
	}

    @Override
    public Page<TaskDefinition> search(SearchPageable searchPageable) {
        Assert.notNull(searchPageable, "searchPageable must not be null.");

        final StringBuilder whereClause = new StringBuilder("WHERE ");
        final List<String> params = new ArrayList<>();
        final String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        if (creator != null) {
            whereClause.append("creator = ? ");
            params.add(creator);
        }
        final Iterator<String> columnIterator = searchPageable.getColumns().iterator();
        if (columnIterator.hasNext()) {
            whereClause.append("AND (");
            while (columnIterator.hasNext()) {
                whereClause.append("lower(" + columnIterator.next()).append(") like ").append("lower(?)");
                params.add("%" + searchPageable.getSearchQuery() + "%");
                if (columnIterator.hasNext()) {
                    whereClause.append(" OR ");
                }
            }
            whereClause.append(")");
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(" ").append(selectClause);
        sql.append(" FROM ").append(tableName);
        sql.append(whereClause == null ? "" : whereClause);

        String query = sql.toString();
        List<TaskDefinition> result = jdbcTemplate.query(query, params.toArray(), rowMapper);
        return queryForPageableResults(searchPageable.getPageable(), selectClause, tableName, whereClause.toString(),
                params.toArray(), result.size());
    }

    @Override
    public Page<TaskDefinition> findAll(Pageable pageable) {
        Assert.notNull(pageable, "pageable must not be null");
        final StringBuilder whereClause = new StringBuilder("");
        final List<String> params = new ArrayList<>();
        final String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        if (creator != null) {
            whereClause.append("WHERE creator = ? ");
            params.add(creator);
        }
        return queryForPageableResults(pageable, selectClause, tableName, whereClause.toString(), params.toArray(), count());
    }
}
