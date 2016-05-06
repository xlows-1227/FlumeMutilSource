package com.flume.source.dirwatchdog;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.util.*;


public class DirWatcher {
    static final Logger LOG = LoggerFactory.getLogger(DirWatcher.class);

    final private List<DirChangeHandler> list = Collections
            .synchronizedList(new ArrayList<DirChangeHandler>());
    private File dir;
    private volatile boolean done = false;
    private Set<File> previous = new HashSet<File>();
    private long sleep_ms;
    private Periodic thread;
    private FileFilter filter;


    public DirWatcher(File dir, FileFilter filter, long checkPeriod) {
        Preconditions.checkNotNull(dir);
        Preconditions.checkArgument(dir.isDirectory(), dir + " is not a directory");

        this.thread = null;
        this.dir = dir;
        this.sleep_ms = checkPeriod;
        this.filter = filter;
    }


    public void start() {
        if (thread != null) {
            LOG.warn("Dir watcher already started!");
            return;
        }
        this.thread = new Periodic();
        this.thread.start();
        LOG.info("Started dir watcher thread");
    }


    public void stop() {
        if (thread == null) {
            LOG.warn("DirWatcher already stopped");
            return;
        }

        done = true;

        try {
            thread.join();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        } // waiting for thread to complete.
        LOG.info("Stopped dir watcher thread");
        thread = null;
    }


    class Periodic extends Thread {
        Periodic() {
            super("DirWatcher");
//            init();
        }

        public void run() {
            try {
                while (!done) {
                    try {
                        check();
                        Thread.sleep(sleep_ms);
                    } catch (NumberFormatException nfe) {
                        LOG.warn("wtf ", nfe);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    public void init(){
        DirChangeHandler[] hs = list.toArray(new DirChangeHandler[0]);
        for (DirChangeHandler h : hs) {
            h.init();
        }
    }


    public void check() {
        // TODO file filter
        File[] files = dir.listFiles(filter);
        if (files == null) { // directory is no longer present
            LOG.info("dir " + dir.getAbsolutePath() + " does not exist!");
            // notifying about files deletion in case there were any
            Set<File> removedFiles = new HashSet<File>(previous);
            for (File f : removedFiles) {
                // filter is not applied to dirs
                if (f.isDirectory() || filter.accept(f)) {
                    fireDeletedFile(f);
                }
            }
            return;
        }
        Set<File> newfiles = new HashSet<File>(Arrays.asList(files));

        // figure out what was created
        Set<File> addedFiles = new HashSet<File>(newfiles);
        addedFiles.removeAll(previous);
        for (File f : addedFiles) {
            // filter is not applied to dirs
            if (f.isDirectory() || filter.accept(f)  || f.getName().contains("flume")) {
                fireCreatedFile(f);
            } else {
                newfiles.remove(f); // don't keep filtered out files
            }
        }

        // figure out what was deleted
        Set<File> removedFiles = new HashSet<File>(previous);
        removedFiles.removeAll(newfiles);
        for (File f : removedFiles) {
            // firing event on every deleted File: filter can NOT be applied
            // since we don't want to filter out directories and f.isDirectory() is
            // always false for removed dir. Anyways, as long as "previous" contains only
            // filtered files (or dirs) no need to apply filter here.
            fireDeletedFile(f);
        }

        previous = newfiles;
    }


    public void addHandler(DirChangeHandler dch) {
        list.add(dch);
    }


    public void fireCreatedFile(File f) {

        // make copy so it is thread safe
        DirChangeHandler[] hs = list.toArray(new DirChangeHandler[0]);
        for (DirChangeHandler h : hs) {
            h.fileCreated(f);
        }
    }


    public void fireDeletedFile(File f) {
        // make copy so it is thread safe
        DirChangeHandler[] hs = list.toArray(new DirChangeHandler[0]);
        for (DirChangeHandler h : hs) {
            h.fileDeleted(f);
        }
    }
    

}
