package com.mlog.datasync.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mlog.datasync.common.Constant;
import com.mlog.datasync.common.Utils;
import com.mlog.datasync.context.TimeHandler;
import com.mlog.datasync.main.ProcessManager;

public class SourceDataManager implements ProcessManager {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private static SourceDataManager instance = null;

	private Map<String, TimeHandler> handlerMap = null;

	public static synchronized SourceDataManager getInstance() {
		if (instance == null) {
			instance = new SourceDataManager();
		}

		return instance;
	}

	@Override
	public void start() {
		handlerMap = new HashMap<String, TimeHandler>();
		
		try {
			TimeHandler handler = null;

			List<String> ruleNameList = Utils.getFileList(Utils.getProperty(Constant.DATABASE_RULE_PATH));
			for (String ruleName : ruleNameList) {
				Thread thread = new Thread(handler = new SourceDataHandler(ruleName));
				thread.start();

				handlerMap.put(ruleName, handler);
			}
		} catch (Exception e) {
			logger.error("", e);
		}

		logger.info("start [{}]", getClass().getSimpleName());
	}

	@Override
	public void close() {
		Constant.RUN = false;
	}

	@Override
	public void address(Map<String, Object> object) {
		try {
			if (object != null) {
				TimeHandler timeHandler = handlerMap.get((String) object.get(RULE_NAME));
				timeHandler.put(object);
			}
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
