package com.flume.source;

import com.google.common.base.Preconditions;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * tail a file on unix platform, replacing tail -F command
 */
public class TailSourceNG extends AbstractSource implements Configurable,
		EventDrivenSource {
	public static final Logger logger = LoggerFactory
			.getLogger(TailSourceNG.class);
	private TailSource tail;
	private String monitorFile;
	private String positionFile;
	private String fileEncode;
	private int batchSize;
	private boolean startFromEnd;
	private String startChar;
	private boolean isAddFileName;

	public SourceCounter sourceCounter;


	@Override
	public void configure(Context context) {
		this.monitorFile = context.getString("monitorFile");
		this.positionFile = context.getString("positionFile", monitorFile);
		this.fileEncode = context.getString("fileEncode", "UTF-8");
		this.batchSize = context.getInteger("batchSize", 100);
		this.startFromEnd = context.getBoolean("startFromEnd", true);
		this.startChar = context.getString("startChar", "-1");
		this.isAddFileName = context.getBoolean("isAddFileName", false);

		Preconditions.checkNotNull(monitorFile == null,
				"Monitoring file is null!!!");

		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}

	}

	@Override
	public void start() {
		File f = new File(monitorFile);

		File positionLog = new File(positionFile);
		// 100 ms between checks
		this.tail = new TailSource(100);
		// Add a new file to the multi tail.
		Cursor c;
		if (startFromEnd) {
			// init cursor positions on first dir check when startFromEnd is set
			// to true
			c = new Cursor(this, sourceCounter, f, f.length(), f.length(),
					f.lastModified(), fileEncode, batchSize, positionLog,
					startChar, isAddFileName);
		} else {
			c = new Cursor(this, sourceCounter, f, fileEncode, batchSize,
					positionLog, startChar, isAddFileName);
		}

		try {
			c.initCursorPos();
		} catch (InterruptedException e) {
			logger.error("Initializing of custom delimiter cursor failed", e);
			c.close();
			return;
		}

		tail.addCursor(c);
		tail.open();
		super.start();
		sourceCounter.start();
		logger.info("TailDir source started");
	}

	@Override
	public void stop() {
		tail.close();
		sourceCounter.stop();
		super.stop();
		logger.info("TailDir source stopped");
	}

}