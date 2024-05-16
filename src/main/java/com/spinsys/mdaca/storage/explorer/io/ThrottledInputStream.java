package com.spinsys.mdaca.storage.explorer.io;

import java.io.IOException;
import java.io.InputStream;

public class ThrottledInputStream extends InputStream {

	  private final static long defaultBytes = 120000; //.1200 MB
	
	  private final InputStream rawStream;
	  private final long maxBytesPerSec;
	  private final long startTime = System.currentTimeMillis();

	  private long bytesRead = 0;
	  private long totalSleepTime = 0;
	  long lastSleepTime = System.currentTimeMillis();
	  long bytesSinceLastSleep = System.currentTimeMillis();

	  private static final long SLEEP_DURATION_MS = 8;

	  public ThrottledInputStream(InputStream rawStream) {
	    this(rawStream, defaultBytes);
	  }

	  public ThrottledInputStream(InputStream rawStream, long maxBytesPerSec) {
	    assert maxBytesPerSec > 0 : "Bandwidth " + maxBytesPerSec + " is invalid"; 
	    this.rawStream = rawStream;
	    this.maxBytesPerSec = maxBytesPerSec;
	  }

	  /** @inheritDoc */
	  @Override
	  public int read() throws IOException {
	    throttle();
	    int data = rawStream.read();
	    if (data != -1) {
	      bytesRead++;
	      bytesSinceLastSleep++;
	    }
	    return data;
	  }

	  /** @inheritDoc */
	  @Override
	  public int read(byte[] b) throws IOException {
	    throttle();
	    int readLen = rawStream.read(b);
	    if (readLen != -1) {
	      bytesRead += readLen;
	      bytesSinceLastSleep += readLen;
	    }
	    return readLen;
	  }

	  /** @inheritDoc */
	  @Override
	  public int read(byte[] b, int off, int len) throws IOException {
	    throttle();
	    int readLen = rawStream.read(b, off, len);
	    if (readLen != -1) {
	      bytesRead += readLen;
	      bytesSinceLastSleep += readLen;
	    }
	    return readLen;
	  }

	  private void throttle() throws IOException {

      // if we've passed the throttling point, sleep for the remainder of the second
      if (bytesSinceLastSleep >= maxBytesPerSec) {

          // calculate time elapsed since last sleep
          long timeElapsed = System.currentTimeMillis() - lastSleepTime;

          // sleep for the remainder of 1 second (if there is a remainder)
          try {
        	//  System.out.println("SLEEPING " + Long.toString(Math.max(SLEEP_DURATION_MS - timeElapsed, 0)));
			Thread.sleep(Math.max(SLEEP_DURATION_MS - timeElapsed, 0));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

          // reset byte count
          bytesSinceLastSleep = 0;

          // reset sleep time
          lastSleepTime = System.currentTimeMillis();
      }
//	    if (getBytesPerSec() > maxBytesPerSec) {
//	      try {
//	        Thread.sleep(SLEEP_DURATION_MS);
//	        totalSleepTime += SLEEP_DURATION_MS;
//	      } catch (InterruptedException e) {
//	        throw new IOException("Thread aborted", e);
//	      }
//	    }
	  }

	  /**
	   * Getter for the number of bytes read from this stream, since creation.
	   * @return The number of bytes.
	   */
	  public long getTotalBytesRead() {
	    return bytesRead;
	  }

	  /**
	   * Getter for the read-rate from this stream, since creation.
	   * Calculated as bytesRead/elapsedTimeSinceStart.
	   * @return Read rate, in bytes/sec.
	   */
	  public long getBytesPerSec() {
	    long elapsed = (System.currentTimeMillis() - startTime);
	    
	    if(elapsed < SLEEP_DURATION_MS) {
	    	return 0;
	    } else {
	      return bytesRead / (elapsed / 1000);
	    }
	  }

	  /**
	   * Getter the total time spent in sleep.
	   * @return Number of milliseconds spent in sleep.
	   */
	  public long getTotalSleepTime() {
	    return totalSleepTime;
	  }

	  /** @inheritDoc */
	  @Override
	  public String toString() {
	    return "ThrottledInputStream{" +
	        "bytesRead=" + bytesRead +
	        ", maxBytesPerSec=" + maxBytesPerSec +
	        ", bytesPerSec=" + getBytesPerSec() +
	        ", totalSleepTime=" + totalSleepTime +
	        '}';
	  }
	}
