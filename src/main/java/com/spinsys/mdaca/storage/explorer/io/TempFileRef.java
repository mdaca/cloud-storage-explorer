package com.spinsys.mdaca.storage.explorer.io;

import java.io.File;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.logging.Logger;


/**
 * A phantom reference to a temporary file allows us to
 * delete temporary files from disk when the Java object
 * pointing to that file is garbage collected.  By using
 * this mechanism, we don't have to wait for deleteOnExit
 * functionality that doesn't happen until the JVM
 * terminates.
 */
class TempFileRef extends PhantomReference<Object> {

	public static final Logger logger =
			Logger.getLogger("com.spinsys.mdaca.storage.explorer.io.TempFileRef");
	
	private final String path;

	TempFileRef(File file, ReferenceQueue<? super Object> queue) {
		super(file, queue);
		this.path = file.getPath();
	}

	/**
	 * Delete a file from disk
	 * @return true if successful; false if not
	 */
	boolean delete() {
		boolean deleted = false;
		try {
			File file = new File(path);
			String cPath = file.getCanonicalPath();

			if (!file.exists()) {
				deleted = true;
				logger.info("delete() = " + deleted + ".  " + cPath +
						" doesn't exist; thread: " + Thread.currentThread());
			}
			else {
				deleted = file.delete();
				logger.info("delete() = " + deleted + " for " + cPath +
						"; thread: " + Thread.currentThread());
			}
		} catch (Exception e) {
			logger.info("Unable to delete " + path + ": " + e.getMessage());
		}
		return deleted;
	}
}