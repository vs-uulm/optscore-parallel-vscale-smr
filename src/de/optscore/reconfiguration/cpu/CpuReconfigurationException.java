package de.optscore.reconfiguration.cpu;

/**
 * Signals that an error occurred during the reconfiguration of the CPU cores.
 * 
 * @author koestler
 *
 */
public class CpuReconfigurationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2123267759572183154L;

	public CpuReconfigurationException(String message, Throwable cause) {
		super(message, cause);
	}

	public CpuReconfigurationException(String message) {
		super(message);
	}

}
