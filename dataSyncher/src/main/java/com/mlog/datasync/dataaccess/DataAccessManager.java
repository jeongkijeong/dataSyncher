package com.mlog.datasync.dataaccess;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mlog.datasync.common.Constant;
import com.mlog.datasync.common.Utils;
import com.mlog.datasync.main.ProcessManager;

public class DataAccessManager implements ProcessManager{
	private Logger logger = LoggerFactory.getLogger(getClass());

	private static DataAccessManager instance = null;

	private Map<String, DataAccessObject> dataAccessObjectFactory = null;

	public static DataAccessManager getInstance() {
		if (instance == null) {
			instance = new DataAccessManager();
		}

		return instance;
	}

	@Override
	public void start() {
		dataAccessObjectFactory = new HashMap<String, DataAccessObject>();

		try {
			List<HashMap<String, Object>> databaseList = Utils.jsonStrToList(Utils.readFile(Utils.getProperty(Constant.DATABASE_INFO_PATH)));

			for (HashMap<String, Object> database : databaseList) {
				String datasource = (String) database.get("datasource");
				if (datasource == null) {
					continue;
				}

				dataAccessObjectFactory.put(datasource, new DataAccessHandler(datasource));
			}
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	public DataAccessObject getDataAccessObject(String dataSource) {
		DataAccessObject dataAccessObject = null;

		if (dataAccessObjectFactory != null) {
			dataAccessObject = dataAccessObjectFactory.get(dataSource);
		}

		if (dataAccessObject != null) {
			logger.debug("success get dataAccessObject : [{}]", dataSource);
		} else {
			logger.error("failure get dataAccessObject : [{}]", dataSource);
		}

		return dataAccessObject;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	@Override
	public void address(Map<String, Object> object) {
		// TODO Auto-generated method stub
	}
}
