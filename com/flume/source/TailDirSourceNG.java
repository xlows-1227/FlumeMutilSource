package com.flume.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.hexun.flume.source.dirwatchdog.DirChangeHandler;
import com.hexun.flume.source.dirwatchdog.DirWatcher;
import com.hexun.flume.source.dirwatchdog.RegexFileFilter;

public class TailDirSourceNG extends AbstractSource implements Configurable,
		EventDrivenSource {
	public static final Logger logger = LoggerFactory
			.getLogger(TailDirSourceNG.class);
	private TailSource tail;
	private DirWatcher watcher;
	private volatile boolean dirChecked = false;

	private String monitorDirPath;
	private String positionLogPath;
	private String fileEncode;
	private String fileNameSeparator;
	private String fileRegex;
	private int batchSize;
	private boolean startFromEnd;
	private boolean readFromLog;
	private String fileNameSkipString;
	private String fileNameSkipEndString;
	private String startChar;
	private boolean isNumberFile;
	private boolean isAddFileName;
	private int overTime;

	public SourceCounter sourceCounter;

	@Deprecated
	private String fileNameDelimRegex;
	@Deprecated
	private String fileNameDelimMode;

	// TODO
	private String fileNameIncludeStrRegex;
	private String fileNameExcludeStrRegex;
	private boolean loopSubDir;

	private int fileNameLength;

	private String mustHave;

	@Override
	public void configure(Context context) {

		this.monitorDirPath = context.getString("monitorPath");
		this.positionLogPath = context.getString("positionLogPath",
				monitorDirPath);
		this.fileEncode = context.getString("fileEncode", "UTF-8");
		this.fileRegex = context.getString("fileRegex", ".*");
		this.batchSize = context.getInteger("batchSize", 100);
		this.startFromEnd = context.getBoolean("startFromEnd", true);
		this.fileNameDelimRegex = context.getString("fileNameDelimRegex", null);
		this.fileNameDelimMode = context.getString("fileNameDelimMode", null);
		this.fileNameSkipString = context.getString("fileNameSkipString",
				"flume");
		this.fileNameSeparator =  context.getString("fileNameSeparator",
				"\t");
		this.fileNameSkipEndString = context.getString("fileNameSkipEndString",
				"1");
		this.fileNameLength = context.getInteger("fileNameLength", 0);
		this.readFromLog = context.getBoolean("readFromLog", false);
		this.loopSubDir = context.getBoolean("loopSubDir", false);
		this.startChar = context.getString("startChar", "-1");
		this.isNumberFile=context.getBoolean("isTailNumberfile",true);
		this.isAddFileName=context.getBoolean("isAddFileName",false);
		this.mustHave=context.getString("mustHave","-1");
		this.overTime=context.getInteger("overTime",7);

		// TODO 待加业务逻辑
		this.fileNameIncludeStrRegex = context
				.getString("fileNameIncludeStrRegex");
		this.fileNameExcludeStrRegex = context
				.getString("fileNameExcludeStrRegex");

		Preconditions.checkNotNull(monitorDirPath == null,
				"Monitoring directory path is null!!!");
		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}

	}

	@Override
	public void start() {
		Preconditions.checkState(watcher == null,
				"Attempting to open an already open TailDirSource ("
						+ monitorDirPath + ", \"" + fileRegex + "\")");
		File positionLog = new File(positionLogPath + "/position.log");
		File positionLogDir=new File(positionLogPath);
		if(!positionLogDir.exists()){
			positionLogDir.mkdirs();
		}
		if (!positionLog.exists()) {
			try {
				positionLog.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// 100 ms between checks
		this.tail = new TailSource(100);
		watcher = createWatcher(new File(monitorDirPath), fileRegex, this,
				sourceCounter, positionLog);
		dirChecked = true;
		watcher.start();
		tail.open();
		super.start();
		sourceCounter.start();
		logger.info("TailDir source started");
	}

	@Override
	public void stop() {
		tail.close();
		this.watcher.stop();
		this.watcher = null;
		sourceCounter.stop();
		super.stop();
		logger.info("TailDir source stopped");
	}

	private DirWatcher createWatcher(File dir, final String regex,
			final TailDirSourceNG source, final SourceCounter sourceCounter,
			final File positionLog) {
		// 250 ms between checks
		DirWatcher watcher = new DirWatcher(dir, new RegexFileFilter(regex),
				250);
		watcher.addHandler(new DirChangeHandler() {
			Map<String, Cursor> curmap = new HashMap<String, Cursor>();

			@Override
			public void fileCreated(File f) {

				if (f.isDirectory()) {
					if (loopSubDir) {
						createWatcher(f, regex, source, sourceCounter,
								positionLog).start();
					}
					return;
				}
				
				if (System.currentTimeMillis()-f.lastModified()>86400000*overTime){
					return;
				}
				
				if(!isNumberFile && hasDigit(f.getName())){
					return;
				}

				if (f.getName().contains("flume")
						|| f.getName().contains(fileNameSkipString)) {
					return;
				}

				if (fileNameLength != 0
						&& f.getName().length() != fileNameLength) {
					return;
				}

				if (!fileNameSkipEndString.equals("1")
						&& !f.getName().endsWith(fileNameSkipEndString)) {
					return;
				}
				
				if(!mustHave.equals("-1")  &&  !f.getName().contains(mustHave)){
					return;
				}

				// Add a new file to the multi tail.
				logger.info("added file " + f);
				
				Cursor c;
				if (fileNameDelimRegex == null) {
					if (startFromEnd && !dirChecked && !readFromLog) {
						// init cursor positions on first dir check when
						// startFromEnd is set
						// to true
						c = new Cursor(source, sourceCounter, f, f.length(), f
								.length(), f.lastModified(), fileEncode,
								batchSize, positionLog, startChar,isAddFileName,fileNameSeparator);
					} else if (readFromLog) {
						c = new Cursor(source, sourceCounter, f, fileEncode,
								batchSize, positionLog, startChar,isAddFileName,fileNameSeparator);
						List<String> tailedFile = readFileByLines(positionLog);
						String filename = f.getAbsolutePath().trim();
						if (!tailedFile.contains(filename)) {
							c = new Cursor(source, sourceCounter, f,
									fileEncode, batchSize, positionLog,
									startChar,isAddFileName,fileNameSeparator);
						} else {
							synchronized (positionLog) {
								BufferedReader reader = null;
								try {
									reader = new BufferedReader(new FileReader(
											positionLog));
									String line = null;
									while ((line = reader.readLine()) != null) {
										if (line.contains(f.getAbsolutePath())
												&& line.split(",").length > 3) {
											long lastReadOffset = Long
													.parseLong(line.split(",")[1]);
											long lastFileLen = Long
													.parseLong(line.split(",")[2]);
											long lastMod = Long.parseLong(line
													.split(",")[3]);
											c = new Cursor(source,
													sourceCounter, f,
													lastReadOffset,
													lastFileLen, lastMod,
													fileEncode, batchSize,
													positionLog, startChar,isAddFileName,fileNameSeparator);
											break;
										}
									}
									reader.close();
								} catch (IOException e) {
									e.printStackTrace();
								} finally {
									if (reader != null) {
										try {
											reader.close();
										} catch (IOException e1) {
										}
									}
								}
							}

						}
					} else {
						c = new Cursor(source, sourceCounter, f, fileEncode,
								batchSize, positionLog, startChar,isAddFileName,fileNameSeparator);
					}
				} else {
					// special delimiter modes
					if (startFromEnd && !dirChecked) {
						// init cursor positions on first dir check when
						// startFromEnd is set
						// to true
						c = new CustomDelimCursor(source, sourceCounter, f,
								fileEncode, batchSize, f.length(), f.length(),
								f.lastModified(), fileNameDelimRegex,
								fileNameDelimMode, positionLog, startChar,isAddFileName,fileNameSeparator);
					} else {
						c = new CustomDelimCursor(source, sourceCounter, f,
								fileEncode, batchSize, fileNameDelimRegex,
								fileNameDelimMode, positionLog, startChar,isAddFileName,fileNameSeparator);
					}
				}

				try {
					c.initCursorPos();
				} catch (InterruptedException e) {
					logger.error("Initializing of cursor failed", e);
					c.close();
					return;
				}
//				logger.warn(f.lastModified()+"");
//				logger.warn(System.currentTimeMillis()+"");
				curmap.put(f.getPath(), c);
				tail.addCursor(c);
				
				//remove old File
				Set<String> curKey = curmap.keySet();
				List<String> hasToRemoveFile=new ArrayList<String>();
				Iterator<String> keys = curKey.iterator();
				while(keys.hasNext()){
					String key=keys.next();
					File tmpFile=new File(key);
					if(System.currentTimeMillis()-tmpFile.lastModified()>100000000){
						hasToRemoveFile.add(key);
					}
				}
				for(int i=0;i<hasToRemoveFile.size();i++){
					String removeFile=hasToRemoveFile.get(i);
					Cursor removeCur=curmap.remove(removeFile);
					if(removeCur!=null){
						logger.warn("removed file " + removeFile);
						tail.removeCursor(removeCur);
					}
				}
				
			}

			@Override
			public void fileDeleted(File f) {
				logger.debug("handling deletion of file " + f);
				String fileName = f.getPath();
				Cursor c = curmap.remove(fileName);
				// this check may seem unneeded but there are cases which it
				// handles,
				// e.g. if unwatched subdirectory was removed c is null.
				if (c != null) {
					logger.info("removed file " + f);
					tail.removeCursor(c);
				}
			}

			@Override
			public void init() {

			}

		});

		// Separate check is needed to init cursor positions
		// (to the end of the files in dir)
		if (startFromEnd) {
			watcher.check();
		}
		return watcher;
	}

	public static List<String> readFileByLines(File file) {
		List<String> tailedList = new ArrayList<String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while ((tempString = reader.readLine()) != null) {
				tailedList.add(tempString.split(",")[0]);
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
		return tailedList;
	}

	public boolean hasDigit(String content) {
		boolean flag = false;
		Pattern p = Pattern.compile(".*\\d+.*");
		Matcher m = p.matcher(content);
		if (m.matches())
			flag = true;
		return flag;

	}
}
