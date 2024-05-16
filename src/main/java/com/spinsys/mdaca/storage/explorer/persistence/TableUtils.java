package com.spinsys.mdaca.storage.explorer.persistence;

public class TableUtils {

    // TODO move to some configuration variable or property
    public static final String STOREXP_PERSISTENT_UNIT = "storexppu";


	/**
	 * Make sure that the size of the value to be saved does not exceed
	 * the numbers of characters allowed.
	 * @param input the value to check
	 * @param max the maximum number of characters wanted
	 * @return the input value when that value isn't too large;
	 *   otherwise, the input value truncated to the desired number
	 *   of characters.
	 */
	public static String getSafeValue(String input, int max) {
		String value = null;
		if (input != null) {
			value = (input.length() > max) ? input.substring(0, max) : input;
		}
		return value;
	}

}
