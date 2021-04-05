package com.mlog.datasync.dataaccess;

import org.apache.ibatis.session.SqlSession;

import com.mlog.datasync.common.CommonStr;

public interface DataAccessObject extends CommonStr {
	public SqlSession getSqlSession();
}
