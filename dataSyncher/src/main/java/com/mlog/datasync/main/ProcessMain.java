package com.mlog.datasync.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mlog.datasync.common.CommonStr;
import com.mlog.datasync.common.Constant;
import com.mlog.datasync.common.Utils;
import com.mlog.datasync.context.ContextManager;

import sun.misc.Signal;
import sun.misc.SignalHandler;


public class ProcessMain implements SignalHandler, CommonStr {
	private static final Logger logger = LoggerFactory.getLogger(ProcessMain.class);

	public static void main(String[] args) throws Exception {
		int retv = -1;

		String cfgFilePath = DEFAULT_CFG_FILE_PATH;
		String logFilePath = DEFAULT_LOG_FILE_PATH;

		if (args != null && args.length >= 2) {
			cfgFilePath = args[0];
			logFilePath = args[1];
		}

		retv = Utils.loadProperties(cfgFilePath);
		if (retv < 0) {
			return;
		}

		retv = Utils.loadLogConfigs(logFilePath);
		if (retv < 0) {
			return;
		}

		ProcessMain processMain = new ProcessMain();
		processMain.startProcess();

		delegateHandler("TERM", processMain);
		while (Constant.RUN) {
			Thread.sleep(3000);
		}
	}

	/**
	 * start main process.
	 * */
	private void startProcess() {
		ContextManager contextManager = ContextManager.getInstance();
		contextManager.startManager();
	}

	/**
	 * close main process.
	 * */
	private void closeProcess() {
		ContextManager contextManager = ContextManager.getInstance();
		contextManager.closeManager();

		System.exit(0);
	}

	@Override
	public void handle(Signal signal) {
		logger.info("Received SIG NAME[" + signal.getName() + "] / NUMB[" + signal.getNumber() + "]");

		String SIGName = signal.getName();
		if (SIGName == null || SIGName.length() == 0) {
			return;
		}

		// end of process
		if (SIGName.equals("TERM")) {
			logger.info("close " + getClass().getName());
			closeProcess();
		}
	}

	public static void delegateHandler(String SIGName, SignalHandler SIGHandler) {
		Signal SIG = null;

		try {
			SIG = new Signal(SIGName);
			SIGHandler = Signal.handle(SIG, SIGHandler);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
