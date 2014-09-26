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

public class StringIntegerList implements Writable {
	public static class StringInteger implements Writable {
		private String s;
		private int t;
		public static Pattern p = Pattern.compile("(.+),(\\d+)");

		public StringInteger() {
		}

		public StringInteger(String s, int t) {
			this.s = s;
			this.t = t;
		}

		public String getString() {
			return s;
		}

		public int getValue() {
			return t;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			String indexStr = arg0.readUTF();

			Matcher m = p.matcher(indexStr);
			if (m.matches()) {
				this.s = m.group(1);
				this.t = Integer.parseInt(m.group(2));
			}
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			StringBuffer sb = new StringBuffer();
			sb.append(s);
			sb.append(",");
			sb.append(t);
			arg0.writeUTF(sb.toString());
		}

		@Override
		public String toString() {
			return s + "," + t;
		}
	}

	private List<StringInteger> indices;
	private Map<String, Integer> indiceMap;
	private Pattern p = Pattern.compile("<([^>]+),(\\d+)>");

	public StringIntegerList() {
		indices = new Vector<StringInteger>();
	}

	public StringIntegerList(List<StringInteger> indices) {
		this.indices = indices;
	}

	public StringIntegerList(Map<String, Integer> indiceMap) {
		this.indiceMap = indiceMap;
		this.indices = new Vector<StringInteger>();
		for (String index : indiceMap.keySet()) {
			this.indices.add(new StringInteger(index, indiceMap.get(index)));
		}
	}

	public Map<String, Integer> getMap() {
		if (this.indiceMap == null) {
			indiceMap = new HashMap<String, Integer>();
			for (StringInteger index : this.indices) {
				indiceMap.put(index.s, index.t);
			}
		}
		return indiceMap;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		String indicesStr = WritableUtils.readCompressedString(arg0);
		readFromString(indicesStr);
	}

	public void readFromString(String indicesStr) throws IOException {
		List<StringInteger> tempoIndices = new Vector<StringInteger>();
		Matcher m = p.matcher(indicesStr);
		while (m.find()) {
			StringInteger index = new StringInteger(m.group(1), Integer.parseInt(m.group(2)));
			tempoIndices.add(index);
		}
		this.indices = tempoIndices;
	}

	public List<StringInteger> getIndices() {
		return Collections.unmodifiableList(this.indices);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeCompressedString(arg0, this.toString());
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < indices.size(); i++) {
			StringInteger index = indices.get(i);
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
