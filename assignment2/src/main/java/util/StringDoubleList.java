package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class StringDoubleList implements Writable {
	public static class StringDouble implements Writable {
		private String s;
		private double d;
		public static Pattern p = Pattern.compile("(.+),([-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?)");

		public StringDouble() {
		}

		public StringDouble(String s, double d) {
			this.s = s;
			this.d = d;
		}

		public String getString() {
			return s;
		}

		public double getValue() {
			return d;
		}

		public void readFields(DataInput arg0) throws IOException {
			String indexStr = arg0.readUTF();

			Matcher m = p.matcher(indexStr);
			if (m.matches()) {
				this.s = m.group(1);
				this.d = Double.parseDouble(m.group(2));
			}
		}

		public void write(DataOutput arg0) throws IOException {
			StringBuffer sb = new StringBuffer();
			sb.append(s);
			sb.append(",");
			sb.append(d);
			arg0.writeUTF(sb.toString());
		}

		@Override
		public String toString() {
			return s + "," + d;
		}
	}

	private List<StringDouble> indices;
	private Map<String, Double> indiceMap;
	private Pattern p = Pattern.compile("<([^>]+),([-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?)>");

	public StringDoubleList() {
		indices = new Vector<StringDouble>();
	}

	public StringDoubleList(List<StringDouble> indices) {
		this.indices = indices;
	}

	public StringDoubleList(Map<String, Double> indiceMap) {
		this.indiceMap = indiceMap;
		this.indices = new Vector<StringDouble>();
		for (String index : indiceMap.keySet()) {
			this.indices.add(new StringDouble(index, indiceMap.get(index)));
		}
	}

	public Map<String, Double> getMap() {
		if (this.indiceMap == null) {
			indiceMap = new HashMap<String, Double>();
			for (StringDouble index : this.indices) {
				indiceMap.put(index.s, index.d);
			}
		}
		return indiceMap;
	}

	public void readFields(DataInput arg0) throws IOException {
		String indicesStr = WritableUtils.readCompressedString(arg0);
		readFromString(indicesStr);
	}

	public void readFromString(String indicesStr) throws IOException {
		List<StringDouble> tempoIndices = new Vector<StringDouble>();
		Matcher m = p.matcher(indicesStr);
		while (m.find()) {
			StringDouble index = new StringDouble(m.group(1), Double.parseDouble(m.group(2)));
			tempoIndices.add(index);
		}
		this.indices = tempoIndices;
	}

	public List<StringDouble> getIndices() {
		return Collections.unmodifiableList(this.indices);
	}

	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeCompressedString(arg0, this.toString());
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < indices.size(); i++) {
			StringDouble index = indices.get(i);
			if (index.getString().contains("<") || index.getString().contains(">"))
				continue;
			sb.append("<");
			sb.append(index.getString());
			sb.append(",");
			sb.append(index.getValue());
			sb.append(">");
			if (i != indices.size() - 1) {
				sb.append(",");
			}
		}
		return sb.toString();
	}
}
