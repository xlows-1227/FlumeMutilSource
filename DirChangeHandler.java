package com.flume.source.dirwatchdog;

import java.io.File;


public interface DirChangeHandler {
	
	void init();

    void fileDeleted(File f);

    void fileCreated(File f);

}
