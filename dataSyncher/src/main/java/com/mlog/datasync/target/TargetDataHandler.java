package com.mlog.datasync.target;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mlog.datasync.common.Utils;
import com.mlog.datasync.context.DataHandler;
import com.mlog.datasync.dataaccess.DataAccessHandler;
import com.mlog.datasync.dataaccess.DataAccessManager;

public class TargetDataHandler extends DataHandler {
	private Logger logger = LoggerFactory.getLogger(getClass());

	private DataAccessHandler dataAccessHandler = null;
	private Map<String, Object> execution = null;

	private String ruleName = null;

	public TargetDataHandler(String path) {
		this.ruleName = path;

		initProcess();
	}

	private void initProcess() {
		try {
			execution = Utils.parseRule(ruleName, TARGET);
			dataAccessHandler = (DataAccessHandler) DataAccessManager.getInstance().getDataAccessObject(execution.get(DATASOURCE).toString());
	      } catch (Exception e) {
	    	  logger.error("", e);
	      }
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handler(Object object) {
		try {
			Map<String, Object> targetData = (Map<String, Object>) object;
			targetData.putAll(execution);

			int count = dataAccessHandler.put(targetData);

			logger.debug("[{}] / [{}] / [{}]", this.getClass().getSimpleName(), ruleName, count);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}

