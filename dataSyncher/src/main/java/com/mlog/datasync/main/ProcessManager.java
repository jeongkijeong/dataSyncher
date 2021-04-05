package com.mlog.datasync.main;

import java.util.Map;

import com.mlog.datasync.common.CommonStr;

public interface ProcessManager extends CommonStr {
	public void start();

	public void close();

	public void address(Map<String, Object> object);
}