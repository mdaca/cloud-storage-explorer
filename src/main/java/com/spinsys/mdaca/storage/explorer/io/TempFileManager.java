package com.spinsys.mdaca.storage.explorer.io;

import java.io.File;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Delete unused files after GC collect the references.
 */
public class TempFileManager {

	public static final Logger logger =
			Logger.getLogger("com.spinsys.mdaca.storage.explorer.io.TempFileReaper");
	
	/** The singleton TempFileManager */
	private static TempFileManager singletonManager = null;

	private volatile TempFileReaperThread reaperThread = null;
	
	/** The singleton reference queue where
	 * the garbage collector places reference objects when
	 * the reference field is cleared (set to null). */
	private static ReferenceQueue<Object> referenceQueue = new ReferenceQueue<>();

	private Set<TempFileRef> references =
			Collections.newSetFromMap(new ConcurrentHashMap<>());


	private TempFileManager() {
		// force use of getInstance()
	}
	
	public static synchronized TempFileManager getInstance() {
		if (singletonManager == null) {
			singletonManager = new TempFileManager();
		}
		return singletonManager;
	}

	/**
	 * Remove a file from disk after all references to this file will be collected by GC
	 *
	 * @param file temporary file
	 */
	public void deleteWhenUnused(File file) {

		// collect all references
		references.add(new TempFileRef(file, referenceQueue));

		// run the thread to delete unused files
		if (reaperThread == null) {
			synchronized (this) {
				if (reaperThread == null) {
					reaperThread = new TempFileReaperThread();
					reaperThread.start();
				}
			}
		}
	}

	/**
	 * This thread monitors the reference queue and deletes
	 * from disk those temporary files whose corresponding
	 * objects are no longer referenced.
	 */
	class TempFileReaperThread extends Thread {

		public TempFileReaperThread() {
			super("TempFileReaperThread");
			setDaemon(true);
			logger.info("starting TempFileReaperThread");
		}

		@Override
		public void run() {
			while (references.size() > 0) {
				try {
					// wait for a new unused file reference
					TempFileRef unusedReference =
							(TempFileRef)referenceQueue.remove();
					references.remove(unusedReference);
					
					// delete file
					unusedReference.delete();
					unusedReference.clear();
				} catch (InterruptedException e) {
					// ignore
				}
			}
			reaperThread = null;
		}
	}

}