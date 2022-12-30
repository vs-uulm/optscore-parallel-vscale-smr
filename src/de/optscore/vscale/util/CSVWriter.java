package de.optscore.vscale.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CSVWriter {

    private FileWriter fileWriter;

    private static final char DEFAULT_SEPARATOR = ',';

    public CSVWriter(File file) {
        try {
            this.fileWriter = new FileWriter(file);
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void writeLine(long[] values) {
        List<String> stringVals = new ArrayList<>(values.length);
        for(long value : values) {
            stringVals.add(Long.toString(value));
        }
        writeLine(stringVals);
    }

    /**
     * Write a set of values as a single CSV line, separated by the default separator
     * @param values the values to be written in the line
     */
    public void writeLine(double[] values) {
        List<String> stringVals = new ArrayList<>(values.length);
        for(double value : values) {
            stringVals.add(Double.toString(value));
        }
        writeLine(stringVals);
    }

    /**
     * Write a set of values as a single CSV line, separated by the default separator
     * @param values the values to be written in the line
     */
    public void writeLine(List<String> values) {
        writeLine(values, DEFAULT_SEPARATOR);
    }

    /**
     * Write a set of values as a single CSV line
     * @param values the values to be written in the line
     * @param separator the character used to separate values
     */
    public void writeLine(List<String> values, char separator) {
        boolean first = true;

        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            if (!first) {
                sb.append(separator);
            }
            sb.append(value);

            first = false;
        }
        sb.append("\n");

        try {
            this.fileWriter.append(sb.toString());
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Simple wrapper to flush the underlying writer's data out to file
     */
    public void flush() {
        try {
            this.fileWriter.flush();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Simple wrapper to close the underlying writer. Writer will be flushed at least once before closing.
     * After calling this method, the CSVWriter can't be used to write out lines anymore.
     */
    public void closeWriter() {
        try {
            this.fileWriter.flush();
            this.fileWriter.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

}