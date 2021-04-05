package com.mlog.datasync.datasource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ibatis.datasource.unpooled.UnpooledDataSourceFactory;

public class MyBasicDataSource extends UnpooledDataSourceFactory {

	public MyBasicDataSource() {
		this.dataSource = new BasicDataSource();
	}
}
