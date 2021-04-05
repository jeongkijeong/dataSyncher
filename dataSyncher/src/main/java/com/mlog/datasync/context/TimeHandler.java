package com.mlog.datasync.context;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.mlog.datasync.common.CommonStr;
import com.mlog.datasync.common.Constant;

public abstract class TimeHandler implements Runnable, CommonStr {
	private ArrayBlockingQueue<Object> queue = null;
	private Boolean doRun = null;

	private int timeout = 30;

	public TimeHandler() {
		super();
		queue = new ArrayBlockingQueue<Object>(100);
		this.doRun = Constant.RUN;
	}

	@Override
	public void run() {
		while (doRun) {
			try {
				Object object = queue.poll(timeout, TimeUnit.SECONDS);

				handler(object);
			} catch (Exception e) {
			}
		}
	}

	public void put(Object object) {
		try {
			queue.put(object);
		} catch (Exception e) {
		}
	}
	
	public void isRun(Boolean run) {
		this.doRun = run;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public abstract void handler(Object object);
}
