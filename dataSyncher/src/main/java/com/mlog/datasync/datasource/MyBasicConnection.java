package com.mlog.datasync.datasource;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mlog.datasync.common.CommonStr;
import com.mlog.datasync.common.Utils;

public class MyBasicConnection implements CommonStr{
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private SqlSessionFactory sessionFactory = null;
	private String dataSource = null;

	public MyBasicConnection() {
		super();
	}

	public MyBasicConnection(String dataSource) {
		super();
		this.dataSource = dataSource;
	}

	public SqlSessionFactory init() {
		try {
			String configPath = "db/config/mybiats_cfg.xml";

			sessionFactory = new SqlSessionFactoryBuilder().build(Resources.getResourceAsReader(configPath),
					Utils.getDataBaseProperties(dataSource));
		} catch (Exception e) {
			logger.error("", e);
		}

		return sessionFactory;
	}

	public SqlSession openSession() {
		SqlSession sqlSession = null;

		if (sessionFactory == null) {
			sessionFactory = init();
		}

		try {
			sqlSession = sessionFactory.openSession();
		} catch (Exception e) {
			logger.error("", e);
		}

		return sqlSession;
	}
}
