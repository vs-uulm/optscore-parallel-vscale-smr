package de.optscore.reconfiguration.cpu;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements CPUReconfigurator functionality for Linux hosts to add and remove
 * CPU cores on the fly.
 */
public class LinuxCpuReconfigurator implements CpuReconfigurator {

	private List<CpuCore> cores;

	public LinuxCpuReconfigurator() throws CpuReconfigurationException {
		this.cores = readAvailableCores();
	}

	@Override
	public List<CpuCore> listAvailableCores()
			throws CpuReconfigurationException {
		return readAvailableCores();
	}

	private List<CpuCore> readAvailableCores()
			throws CpuReconfigurationException {
		List<CpuCore> cores = new ArrayList<>();
		try {
			DirectoryStream<Path> stream = Files
					.newDirectoryStream(LinuxCpuCore.CPU_DIRECTORY, "cpu[0-9]*");

			for (Path path : stream) {
				int index = Integer
						.parseInt(path.getFileName().toString().substring(3));

				cores.add(new LinuxCpuCore(index));
			}
			stream.close();
			return cores;
		} catch (IOException cause) {
			throw new CpuReconfigurationException(
					"Could not read cpu information from the device filesystem!",
					cause);
		}
	}

	public void addCpuCore() throws CpuReconfigurationException {
		for (CpuCore core : cores) {
			if (!core.isOnline()) {
				core.setOnline(true);
				return;
			}
		}

		throw new CpuReconfigurationException(
				"Could not activate additional core!");
	}

	public void removeCpuCore() throws CpuReconfigurationException {
		for (int i = cores.size() - 1; i > 0; i--) {
			CpuCore core = cores.get(i);
			if (core.isOnline()) {
				core.setOnline(false);
			}
		}

		throw new CpuReconfigurationException("Could not deactivate!");
	}

	public void addCpuCores(int count) throws CpuReconfigurationException {
		for (int i = 0; i < count; i++) {
			addCpuCore();
		}
	}

	public void removeCpuCores(int count) throws CpuReconfigurationException {
		for (int i = 0; i < count; i++) {
			removeCpuCore();
		}
	}

	public int numberOfActiveCpuCores() throws CpuReconfigurationException {
		int active = 0;

		for (CpuCore core : cores) {
			if (core.isOnline()) {
				active++;
			}
		}
		return active;
	}

	@Override
	public int numberOfAvailableCpuCores() throws CpuReconfigurationException {
		return cores.size();
	}
}
