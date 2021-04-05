package com.mlog.datasync.dataaccess;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mlog.datasync.datasource.MyBasicConnection;

public class DataAccessHandler implements DataAccessObject {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private String dataSource = null;
	private MyBasicConnection connection = null;

	public DataAccessHandler(String dataSource) {
		super();
		this.dataSource = dataSource;
	}

	public SqlSession getSqlSession() {
		if (connection == null) {
			connection = new MyBasicConnection(dataSource);
		}

		SqlSession sqlSession = connection.openSession();
		return sqlSession;
	}

	public List<Map<String, Object>> get(Map<String, Object> object) {
		List<Map<String, Object>> selectedList = new ArrayList<Map<String, Object>>();

		Connection conn = getSqlSession().getConnection();
		
		try {
 			String query = (String) object.get(SQL);
			Map<String, List<String>> convert = variableConvertor(query);

			List<String> variableList = convert.get(VARIABLE);
			for (String variable : variableList) {
				query = query.replaceAll(variable, "?");
			}

			PreparedStatement preparedStatement = conn.prepareStatement(query);
			ResultSetMetaData resultSetMetaData = preparedStatement.getMetaData();
			int count = resultSetMetaData.getColumnCount();
			
			// execute select query!
			ResultSet resultSet = preparedStatement.executeQuery();

			// collect selected data from result set object.
			while (resultSet.next()) {
				Map<String, Object> selectedData = new HashMap<String, Object>();
				for (int i = 1; i <= count; i++) {
					selectedData.put(resultSetMetaData.getColumnName(i), resultSet.getObject(i));
				}

				selectedList.add(selectedData);
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				logger.error("", e);
			}
		}

		return selectedList;
	}
	
	@SuppressWarnings("unchecked")
	public int put(Map<String, Object> object) {
		int retv = 0;
		
		Connection conn = getSqlSession().getConnection();

		try {
			String query = (String) object.get(SQL);
			
			List<Map<String, Object>> sqlData = (List<Map<String, Object>>) object.get(RULE_DATA);
			Map<String, List<String>> convert = variableConvertor(query);

			List<String> variableList = convert.get(VARIABLE);
			List<String> striptedList = convert.get(STRIPTED);
			
			for (String variable : variableList) {
				query = query.replace(variable, "?");
			}

			PreparedStatement pstm = conn.prepareStatement(query);
			int size = striptedList.size();
			
			for (Map<String, Object> data : sqlData) {
				for (int index = 0; index < size; index++) {
					pstm.setObject(index + 1, data.get(striptedList.get(index)));
				}

				pstm.addBatch();
				pstm.clearParameters();
				
				if (retv++ % 1000 == 0) {
					pstm.executeBatch();
				}
			}

			pstm.executeBatch();
			conn.commit();
		} catch (Exception e) {
			logger.error("", e);
			retv = FAILURE;
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				logger.error("", e);
			}
		}

		return retv;
	}

	public Map<String, List<String>> variableConvertor(String query) {
		List<String> variableList = new ArrayList<String>();
		List<String> striptedList = new ArrayList<String>();

		Pattern pattern = Pattern.compile("\\#\\{([a-zA-Z0-9$_][:a-zA-Z0-9$_]*)\\}");
		Matcher matcher = pattern.matcher(query);

		while (matcher.find()) {
			String variable = matcher.group(0);
			String stripted = matcher.group(1);

			variableList.add(variable);
			striptedList.add(stripted);
		}

		Map<String, List<String>> convert = new HashMap<String, List<String>>();
		convert.put(STRIPTED, striptedList);
		convert.put(VARIABLE, variableList);

		return convert;
	}

	public Map<String, List<String>> whereClause(String string) {
		String where = null;
		String query = null;
		
		try {
			if (string == null) {
				return null;
			}

			query = string.toLowerCase();

			int s = query.lastIndexOf("where");
			int x = query.lastIndexOf("order");
			int y = query.lastIndexOf("group");
			int z = query.lastIndexOf("having");

			if (s > 0) {
				int stt = s;
				int end = Math.max(Math.max(x, y), z);
				if (end < 0) {
					end = string.length();
				}

				where = string.substring(stt, end);
			}

			if (where != null) {
				Map<String, List<String>> convert = variableConvertor(where);
				return convert;
			}
		} catch (Exception e) {
			logger.error("", e);
		}

		return null;
	}
}
