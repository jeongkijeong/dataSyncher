package com.mlog.datasync.context;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.mlog.datasync.common.CommonStr;
import com.mlog.datasync.common.Constant;

public abstract class DataHandler implements Runnable, CommonStr {
	private ArrayBlockingQueue<Object> queue = null;
	private Boolean doRun = null;
	
	public DataHandler() {
		super();
		queue = new ArrayBlockingQueue<Object>(100);
		this.doRun = Constant.RUN;
	}

	@Override
	public void run() {
		while (doRun) {
			try {
				Object object = queue.poll(3, TimeUnit.SECONDS);
				if (object == null) {
					continue;
				}

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

	public int dataSize() {
		int size = 0;

		if (queue != null) {
			size = queue.size();
		}

		return size;
	}

	public abstract void handler(Object object);
}
