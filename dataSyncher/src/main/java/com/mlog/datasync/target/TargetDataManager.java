package com.mlog.datasync.target;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mlog.datasync.common.Constant;
import com.mlog.datasync.common.Utils;
import com.mlog.datasync.context.DataHandler;
import com.mlog.datasync.main.ProcessManager;

public class TargetDataManager implements ProcessManager {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private static TargetDataManager instance = null;

	private Map<String, DataHandler> handlerMap = null;
	
	public static synchronized TargetDataManager getInstance() {
		if (instance == null) {
			instance = new TargetDataManager();
		}

		return instance;
	}

	@Override
	public void start() {
		handlerMap = new HashMap<String, DataHandler>();
		
		try {
			DataHandler handler = null;
			
			List<String> ruleList = Utils.getFileList(Utils.getProperty(Constant.DATABASE_RULE_PATH));
			for (String rule : ruleList) {
				Thread thread = new Thread(handler = new TargetDataHandler(rule));
				thread.start();

				handlerMap.put(rule, handler);
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
	public synchronized void address(Map<String, Object> object) {
		try {
			if (object != null) {
				DataHandler dataHandler = handlerMap.get((String) object.get(RULE_NAME));
				dataHandler.put(object);
			}
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
