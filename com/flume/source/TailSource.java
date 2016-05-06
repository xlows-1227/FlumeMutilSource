package com.flume.source;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class TailSource {
    private static final Logger LOG = LoggerFactory.getLogger(TailSource.class);

    private static int thdCount = 0;
    private volatile boolean done = false;

    private final long sleepTime; // millis
    final List<Cursor> cursors = new ArrayList<Cursor>();
    private final List<Cursor> newCursors = new ArrayList<Cursor>();
    private final List<Cursor> rmCursors = new ArrayList<Cursor>();

    private TailThread thd = null;


    public TailSource(long waitTime) {
        this.sleepTime = waitTime;
    }


    class TailThread extends Thread {

        TailThread() {
            super("TailThread-" + thdCount++);
        }

        @Override
        public void run() {
            try {
                // initialize based on initial settings.
                for (Cursor c : cursors) {
                    c.initCursorPos();
                }

                while (!done) {
                    synchronized (newCursors) {
                        cursors.addAll(newCursors);
                        newCursors.clear();
                    }

                    synchronized (rmCursors) {
                        cursors.removeAll(rmCursors);
                        for (Cursor c : rmCursors) {
                            c.flush();
                        }
                        rmCursors.clear();
                    }

                    boolean madeProgress = false;
                    for (Cursor c : cursors) {
                        LOG.debug("Progress loop: " + c.file);
                        if (c.tailBody()) {
                            madeProgress = true;
                        }
                    }

                    if (!madeProgress) {
                        Thread.sleep(sleepTime);
                    }
                }
                LOG.debug("Tail got done flag");
            } catch (InterruptedException e) {
                LOG.error("Tail thread nterrupted: " + e.getMessage(), e);
            } finally {
                LOG.info("TailThread has exited");
            }

        }
    }


    public synchronized void addCursor(Cursor cursor) {
        Preconditions.checkArgument(cursor != null);

        if (thd == null) {
            cursors.add(cursor);
            LOG.debug("Unstarted Tail has added cursor: " + cursor.file.getName());

        } else {
            synchronized (newCursors) {
                newCursors.add(cursor);
            }
            LOG.debug("Tail added new cursor to new cursor list: "
                    + cursor.file.getName());
        }

    }


    synchronized public void removeCursor(Cursor cursor) {
        Preconditions.checkArgument(cursor != null);
        if (thd == null) {
            cursors.remove(cursor);
        } else {

            synchronized (rmCursors) {
                rmCursors.add(cursor);
            }
        }

    }

    public void close() {
        synchronized (this) {
            done = true;
            if (thd == null) {
                LOG.warn("TailSource double closed");
                return;
            }
            try {
                while (thd.isAlive()) {
                    thd.join(100L);
                    thd.interrupt();
                }
            } catch (InterruptedException e) {
                LOG.error("Tail source Interrupted Exception: " + e.getMessage(), e);
            }
            thd = null;
        }
    }

    synchronized public void open() {
        if (thd != null) {
            throw new IllegalStateException("Attempted to open tail source twice!");
        }
        thd = new TailThread();
        thd.start();
    }


}
