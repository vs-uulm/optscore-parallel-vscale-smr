package de.optscore.reconfiguration.cpu;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LinuxCpuCore implements CpuCore {

	/* package */ static final Path CPU_DIRECTORY = Paths
			.get("/sys/devices/system/cpu/");

	private static final String ONLINE_STATUS_FILE = "online";

	private static final String CPU_DIRECTORY_PREFIX = "cpu";

	private static final byte ONLINE_BYTE = 49;

	private static final byte OFFLINE_BYTE = 48;

	private static final byte NEWLINE_BYTE = 10;

	private static final byte[] ONLINE_INDICATOR = new byte[] { ONLINE_BYTE,
			NEWLINE_BYTE };

	private static final byte[] OFFLINE_INDICATOR = new byte[] { OFFLINE_BYTE,
			NEWLINE_BYTE };

	private final int index;

	private final Path onlineStatusFile;

	public LinuxCpuCore(int index) {
		if (index < 0 || index >= 1024) {
			throw new IllegalArgumentException(
					"Cpu index must be positive and smaller than 1024!");
		}
		this.index = index;

		if (index == 0) {
			this.onlineStatusFile = null;
		} else {
			this.onlineStatusFile = Paths.get(CPU_DIRECTORY.toString(),
					String.format("%s%d", CPU_DIRECTORY_PREFIX, index),
					ONLINE_STATUS_FILE);
			if (!Files.exists(onlineStatusFile)) {
				throw new IllegalArgumentException(String.format(
						"There is no online status file for cpu with index %d",
						index));
			}
		}
	}

	@Override
	public boolean isOnline() throws CpuReconfigurationException {
		if (index == 0) {
			return true;
		}

		try {
			return (Files.readAllBytes(onlineStatusFile)[0] == 49);
		} catch (IOException cause) {
			throw new CpuReconfigurationException(
					"Could not read online status!", cause);
		}
	}

	@Override
	public void setOnline(boolean isOnline) throws CpuReconfigurationException {
		if (index == 0) {
			return;
		}

		try {
			Files.write(onlineStatusFile,
					isOnline ? ONLINE_INDICATOR : OFFLINE_INDICATOR);
		} catch (IOException cause) {
			throw new CpuReconfigurationException(
					"Could not write online status!", cause);

		}

	}

	@Override
	public int getIndex() {
		return index;
	}

	@Override
	public int hashCode() {
		int result = 17;

		result = 37 * result + index;

		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof LinuxCpuCore)) {
			return false;
		}

		LinuxCpuCore other = (LinuxCpuCore) obj;
		return other.index == this.index;
	}

	@Override
	public String printStatus() throws CpuReconfigurationException {
		return String.format("LinuxCpuCore #%d (%s)", index,
				isOnline() ? "online" : "offline");
	}

}
