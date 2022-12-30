package de.optscore.reconfiguration.cpu;

public interface CpuCore {

	public boolean isOnline() throws CpuReconfigurationException;

	public void setOnline(boolean isOnline) throws CpuReconfigurationException;

	public int getIndex();

	public String printStatus() throws CpuReconfigurationException;
}
