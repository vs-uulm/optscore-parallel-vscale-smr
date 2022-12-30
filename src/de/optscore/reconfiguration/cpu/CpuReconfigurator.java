package de.optscore.reconfiguration.cpu;

import java.util.List;

public interface CpuReconfigurator {

	public List<CpuCore> listAvailableCores()
			throws CpuReconfigurationException;

	/**
	 * Adds one CPU core to this system.
	 * <p>
	 * Should be available to the JVM immediately. Method should block and
	 * return only when the added core has been fully activated and is available
	 * to the JVM. If the hardware does not support any more active cores than
	 * those currently active.
	 */
	public void addCpuCore() throws CpuReconfigurationException;

	/**
	 * Immediately removes one CPU Core from the system, by signalling the OS to
	 * disable the currently highest active core. Method should block and return
	 * once the core has been deactivated. If there aren't enough cores left to
	 * deactivate, //TODO throw some kind of exception...
	 */
	public void removeCpuCore() throws CpuReconfigurationException;

	/**
	 * Add a number of cores to the system. //TODO blocking/non-blocking? //TODO
	 * exception when not enough cores can be activated
	 * 
	 * @param count
	 *            The number of cores that are to be added
	 */
	public void addCpuCores(int count) throws CpuReconfigurationException;

	/**
	 * Remove a number of cores from the system. //TODO blocking/non-blocking?
	 * //TODO exceptions if not enough cores present
	 * 
	 * @param count
	 *            The number of cores that are to be deactivated
	 */
	public void removeCpuCores(int count) throws CpuReconfigurationException;

	/**
	 * Returns the number of currently active CPU cores in this system.
	 * 
	 * @return number of active CPU cores
	 */
	public int numberOfActiveCpuCores() throws CpuReconfigurationException;

	/**
	 * Returns the number of currently active CPU cores in this system.
	 * 
	 * @return number of active CPU cores
	 */
	public int numberOfAvailableCpuCores() throws CpuReconfigurationException;
}
