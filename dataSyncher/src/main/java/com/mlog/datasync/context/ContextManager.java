package com.mlog.datasync.context;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mlog.datasync.common.CommonStr;
import com.mlog.datasync.common.Constant;
import com.mlog.datasync.dataaccess.DataAccessManager;
import com.mlog.datasync.main.ProcessManager;
import com.mlog.datasync.source.SourceDataManager;
import com.mlog.datasync.target.TargetDataManager;

public class ContextManager implements CommonStr {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private List<ProcessManager> managerList = null;
	
	public static ContextManager instance = null;

	public static ContextManager getInstance() {
		if (instance == null) {
			instance = new ContextManager();
		}

		return instance;
	}

	public ContextManager() {
		super();

		managerList = new ArrayList<ProcessManager>();

		managerList.add(DataAccessManager.getInstance());

		managerList.add(TargetDataManager.getInstance()); // create target data manager.
		managerList.add(SourceDataManager.getInstance()); // create source data manager.
	}

	/**
	 * start context manager.
	 */
	public int startManager() {
		logger.info(this.getClass().getSimpleName() + " start");

		Constant.RUN = true;
		try {
			for (ProcessManager manager : managerList) {
				manager.start();
			}
		} catch (Exception e) {
			logger.error("", e);
		}

		logger.info(this.getClass().getSimpleName() + " start completed");
		
		return -1;
	}

	/**
	 * close context manager.
	 */
	public int closeManager() {
		logger.info(this.getClass().getSimpleName() + " close");

		Constant.RUN = false;
		
		try {
			for (ProcessManager manager : managerList) {
				manager.close();
			}
		} catch (Exception e) {
			logger.error("", e);
		}

		logger.info(this.getClass().getSimpleName() + " close completed");
		System.exit(0);
		
		return -1;
	}
}
