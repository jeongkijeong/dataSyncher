package com.mlog.datasync.common;

public interface CommonStr {
	public final String DEFAULT_CFG_FILE_PATH = "./conf/server.properties";
	public final String DEFAULT_LOG_FILE_PATH = "./conf/logback.xml";

	public final static int SUCCESS = +0;
	public final static int FAILURE = -1;

	public final String STRIPTED = "STRIPTED";
	public final String VARIABLE = "VARIABLE";

	public final String SQL = "SQL";
	public final String SQL_DATA = "SQL_DATA";

	public final String SOURCE = "SOURCE";
	public final String TARGET = "TARGET";

	public final String DATASOURCE = "DATASOURCE";
	
	public final String CONDITION = "CONDITION";
	public final String RULE_NAME = "RULE_NAME";
	public final String RULE_DATA = "RULE_DATA";

	public final String MAX   = "MAX";
	public final String MIN   = "MIN";
	public final String LAST  = "LAST";
	public final String FIRST = "FIRST";

	public final String CONDITION_TYPE = "CONDITION_TYPE";
	public final String CONDITION_NAME = "CONDITION_NAME";
	public final String CONDITION_DATA = "CONDITION_DATA";
	public final String CONDITION_ALL  = "CONDITION_ALL";
}
