package com.spinsys.mdaca.storage.explorer.io;

import javax.ws.rs.core.MultivaluedMap;

/**
 * This class provides utilities to manipulate strings
 * that represent paths for various providers.
 * The MDACA StorageExplorer GUI uses a "/" to represent
 * a file separator.
 * @author Keith Cassell
 *
 */
public class PathProcessor {

	/** The separator between path elements (files, dirs)
	 * as received from the StorageExplorer GUI in an HTTP request. */
	public static final String GUI_SEP = "/";

	/** The separator between path elements (files, dirs)
	 * on the computer running this code. */
	public static final String LOCAL_SEP =
			System.getProperty("file.separator", "/");

	/** The separator between path elements (files, dirs)
	 * on UNIX-like systems. */
	public static final String UNIX_SEP = "/";

	/** The separator between path elements (files, dirs)
	 * on Windows-like systems. */
	public static final String WINDOWS_SEP = "\\";

	public static final byte CARRIAGE_RETURN = (byte) '\r';
	public static final byte NEW_LINE = (byte) '\n';

	/**
	 * Returns just the file name without the preceding
	 * directory path info
	 * @param path
	 * @return
	 */
	public static String getFileName(String path) {
	    String fileName = null;
	    
	    if (path != null) {
			if (isDirectory(path)) {
				path = path.substring(0, path.length() - 1);
			}

			int fileSepIndex = path.lastIndexOf(GUI_SEP);

			if (fileSepIndex < 0) { // just a local file name
				fileName = path;
			} else { // a full path - separate the dir and file names
				fileName = path.substring(fileSepIndex + 1);
			}
	    }
	    return fileName;
	}

	/**
	 * @param fileName a file name 
	 * @return extension for the associated file
	 */
	public static String getExtension(String fileName) {
		String ext = null;
		
		if (fileName != null) {
			int dotIndex = fileName.lastIndexOf(".");
			ext = dotIndex < 0 ? "" : fileName.substring(dotIndex + 1);
		}
		return ext;
	}
	
	public static boolean isDirectory(String path) {
		return path.endsWith(GUI_SEP);
	}

	/**
	 * @param filePath file path of the file or folder
	 * @return the parent folder for the given file path
	 * ex. ("C:/folderA/folderB/file.txt" returns "C:/folderA/folderB")
	 */
	public static String getParentFolder(String filePath) {
		if (filePath == null || filePath.isEmpty() || !filePath.contains(GUI_SEP)) {
			return GUI_SEP;
		}
		return filePath.substring(0, filePath.lastIndexOf(GUI_SEP));
	}

	public static String getParentFolderPath(String filePath) {
		filePath = removeFirstSlash(removeLastSlash(filePath));

		if (isRoot(filePath)) {
			return GUI_SEP;
		}
		//if it doesn't contain a slash anymore, then it must be at the root
		if (!filePath.contains(GUI_SEP)) {
			return GUI_SEP;
		}
		return filePath.substring(0, filePath.lastIndexOf(GUI_SEP) + 1);
	}

	public static String getFileNameFromHeader(MultivaluedMap<String, String> header) {

		String[] contentDisposition = header.getFirst("Content-Disposition").split(";");

		for (String filename : contentDisposition) {
			if ((filename.trim().startsWith("filename"))) {
				String[] name = filename.split("=");
				String finalFileName = name[1].trim().replaceAll("\"", "");
				return finalFileName;
			}
		}
		return "unknown";
	}

	public static String addFirstSlash(String s) {
		return ((s == null) || s.startsWith(GUI_SEP)) ?
				s : GUI_SEP + s;
	}

	public static String addLastSlash(String s) {
		return ((s == null) || s.endsWith(GUI_SEP)) ?
				s : s + GUI_SEP;
	}

	public static String removeFirstSlash(String s) {
		return ((s == null) || !s.startsWith(GUI_SEP)) ?
				s : s.substring(1);
	}

	public static String removeLastSlash(String s) {
		return ((s == null) || !s.endsWith(GUI_SEP)) ?
				s : s.substring(0, s.length()-1);
	}

	public static String addBothSlashes(String s) {
		return addFirstSlash(addLastSlash(s));
	}

	public static String removeBothSlashes(String s) {
		return removeFirstSlash(removeLastSlash(s));
	}

	public static String convertToUnixStylePath(String input) {
		String output = null;
		
		if (input != null) {
			output = input.replace(WINDOWS_SEP, UNIX_SEP);
		}
		return output;
	}

	public static String convertToWindowsStylePath(String input) {
		String output = null;
		
		if (input != null) {
			output = input.replace(UNIX_SEP, WINDOWS_SEP);
		}
		return output;
	}

	/**
	 * @return true IFF the string starts with a drive letter
	 * (ex: "C:/folder/file.txt" returns true)
	 */
	public static boolean startsWithDriveLetter(String s) {
		return (s != null) && s.matches("^([a-zA-Z][:][/\\\\]).*");
	}

	public static String removeDriveLetter(String path) {
		return startsWithDriveLetter(path) ? path.substring(3) : path;
	}

	public static boolean sameSourceAndDestination(int sourceDriveId, String sourcePath, int destDriveId, String destPath) {
		boolean same =
				(sourceDriveId == destDriveId)
				&& (sourcePath.equals(destPath)
						// The below handles the case where there is a
						// trailing slash on sourcePath but not destPath
					|| ((sourcePath.length() == destPath.length() + 1)
							&& sourcePath.endsWith(GUI_SEP)
							&& sourcePath.startsWith(destPath)));
		return same;
	}

	public static boolean isRoot(String path) {
		return (path == null) || "".equals(path) || "/".equals(path);
	}

	public static boolean matchesPath(String p1, String p2) {
		p1 = removeBothSlashes(p1);
		p2 = removeBothSlashes(p2);

		return p1.equals(p2);
	}

	public static boolean isNewLine(byte b) {
		return b == NEW_LINE || b == CARRIAGE_RETURN;
	}


}
