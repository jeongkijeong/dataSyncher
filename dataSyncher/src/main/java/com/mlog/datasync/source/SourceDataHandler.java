package com.mlog.datasync.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mlog.datasync.common.Utils;
import com.mlog.datasync.context.TimeHandler;
import com.mlog.datasync.dataaccess.DataAccessHandler;
import com.mlog.datasync.dataaccess.DataAccessManager;
import com.mlog.datasync.target.TargetDataManager;

public class SourceDataHandler extends TimeHandler {
	private Logger logger = LoggerFactory.getLogger(getClass());

	private DataAccessHandler dataAccessHandler = null;
	private Map<String, Object> execution = null;

	private String exeQuery = null;
	private String ruleName = null;

	private List<Map<String, Object>> conditionList = null;

	public SourceDataHandler(String path) {
		this.ruleName = path;

		initProcess();
	}

	private void initProcess() {
		try {
			execution = Utils.parseRule(ruleName, SOURCE);
			dataAccessHandler = (DataAccessHandler) DataAccessManager.getInstance().getDataAccessObject(execution.get(DATASOURCE).toString());

			parseWhereClause();
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	@Override
	public void handler(Object object) {
		try {
			int count = 0;

			List<Map<String, Object>> selectedList = dataAccessHandler.get(execution);
			if (selectedList == null || (count = selectedList.size()) == 0) {
				return;
			}

			delivery(selectedList);

			for (Map<String, Object> condition : conditionList) {
				String data = getCondition(selectedList, (String) condition.get(CONDITION_NAME), (String) condition.get(CONDITION_TYPE));
				condition.put(CONDITION_DATA, data);
			}

			findWhereClauseAndReplace();

			logger.debug("[{}] / [{}] / [{}]", this.getClass().getSimpleName(), ruleName, count);
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	private void delivery(List<Map<String, Object>> data) {
		try {
			Map<String, Object> object = new HashMap<String, Object>();
			object.put(RULE_NAME, ruleName);
			object.put(RULE_DATA, data);

			TargetDataManager.getInstance().address(object);
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	private void parseWhereClause() {
		if (conditionList == null) {
			conditionList = new ArrayList<Map<String, Object>>();
		}

		exeQuery = (String) execution.get(SQL);
		Map<String, List<String>> whereVariable = dataAccessHandler.whereClause(exeQuery);
		if (whereVariable == null) {
			return;
		}

		List<String> variableList = whereVariable.get(VARIABLE);
		List<String> striptedList = whereVariable.get(STRIPTED);

		int size = striptedList.size();
		
		for (int i = 0; i < size; i++) {
			String[] split = striptedList.get(i).split(":");

			Map<String, Object> condition = new HashMap<>();
			condition.put(CONDITION_DATA, split[0]);
			condition.put(CONDITION_ALL, variableList.get(i));

			if (split.length >= 2) {
				condition.put(CONDITION_NAME, split[1]);
				condition.put(CONDITION_TYPE, split[2]);
			}

			conditionList.add(condition);
		}

		findWhereClauseAndReplace();
	}

	private String getCondition(List<Map<String, Object>> list, String name, String type) {
		String result = null;

		long data = 0;

		int size = list.size();
		int indx = 0;

		try {
			switch (type) {
			case FIRST:
				indx = 0;
				result = (String) list.get(indx).get(name);

				break;
			case LAST:
				indx = size - 1;
				result = (String) list.get(indx).get(name);

				break;
			case MAX:
				data = list.stream().map(m -> m.get(name))
						.flatMapToLong(num -> LongStream.of(Long.parseLong(((String) num).trim()))).max().getAsLong();

				result = String.valueOf(data);
				break;
			case MIN:
				data = list.stream().map(m -> m.get(name))
						.flatMapToLong(num -> LongStream.of(Long.parseLong(((String) num).trim()))).min().getAsLong();

				result = String.valueOf(data);
				break;
			default:
				break;
			}

			logger.debug("condition type : [{}] / result : [{}]", type, result);

		} catch (Exception e) {
			logger.error("", e);
		}

		return result;
	}

	private void findWhereClauseAndReplace() {
		String string = exeQuery;
		
		for (Map<String, Object> condition : conditionList) {
			string = findWhereClauseAndReplace(string, (String) condition.get(CONDITION_ALL), (String) condition.get(CONDITION_DATA));
		}

		execution.put(SQL, string);
	}

	private String findWhereClauseAndReplace(String query, String source, String target) {
		String result = null;

		try {
			int stt = query.indexOf(source);
			if (stt < 0) {
				return null;
			}

			int end = stt + source.length();
			int len = query.length();

			result = query.substring(0, stt) + target + query.substring(end, len);
		} catch (Exception e) {
			logger.error("", e);
		}

		return result;
	}

}
