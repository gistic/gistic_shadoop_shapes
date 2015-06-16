package org.gistic.spatialHadoop.OSM_data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * @author ammar
 */
public class RectangleRecord extends Rectangle {
  public int nodeId;
  public int tableId;
  public String tags;
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(nodeId);
    out.writeInt(tableId);
    super.write(out);
    out.writeUTF(tags);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
	nodeId = in.readInt();
	tableId = in.readInt();
    super.readFields(in);
    tags = in.readUTF();
  }

  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeInt(nodeId, text, ',');
    TextSerializerHelper.serializeInt(tableId, text, ',');
    super.toText(text);
    text.append(tags.getBytes(), 0, tags.getBytes().length);
    return text;
  }

  @Override
  public void fromText(Text text) {
    nodeId = TextSerializerHelper.consumeInt(text, ',');
    tableId = TextSerializerHelper.consumeInt(text, ',');
    super.fromText(text);
    tags = new String(text.getBytes());
  }

  @Override
  public String toString() {
    return "#" + nodeId + ", " + "#" + tableId + super.toString() + ", " + tags;
  }
}