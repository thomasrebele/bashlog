package common;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

public class TSVWriter {

	private BufferedWriter br;

	public TSVWriter(String path) throws IOException {
		br = new BufferedWriter(new FileWriter(path));
	}

	public void close() throws IOException {
		br.close();
	}

	public void write(Collection<String> values) throws IOException {
		if (values == null || values.isEmpty())
			throw new IllegalArgumentException();
		StringBuilder sb = new StringBuilder();
		int i = 1;
		for (String value : values) {
			if (i == values.size())
				sb.append(value);
			else
				sb.append(value).append("\t");
			i++;
		}
		sb.append("\n");
		br.write(sb.toString());
	}

}
