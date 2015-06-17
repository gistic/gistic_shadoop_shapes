package org.gistic.spatialHadoop.OSM_data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;
import org.apache.commons.codec.binary.Base64;

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
    byte[] data = Base64.encodeBase64(ByteBuffer.allocate(32).putDouble(x1)
      .putDouble(y1).putDouble(x2).putDouble(y2).array());
    text.append(data, 0, data.length);
    text.append(new byte[] {(byte)','}, 0, 1);
    text.append(tags.getBytes(), 0, tags.getBytes().length);
    return text;
  }

  @Override
  public void fromText(Text text) {
    nodeId = TextSerializerHelper.consumeInt(text, ',');
    tableId = TextSerializerHelper.consumeInt(text, ',');
    byte[] textData = text.getBytes();
    int index = 0;
    while (textData[index] != ',' && index < textData.length) {
      index++;
    }
    byte[] data = new byte[index];
    System.arraycopy(textData, 0, data, 0, index);
    data = Base64.decodeBase64(data);
    long temp = 0;
    for (int i = 0; i < 8; i++)
    	temp = (temp << 8) | (data[i] & 0xff);
    x1 = Double.longBitsToDouble(temp);
    temp = 0;
    for (int i = 8; i < 16; i++)
    	temp = (temp << 8) | (data[i] & 0xff);
    y1 = Double.longBitsToDouble(temp);
    temp = 0;
    for (int i = 16; i < 24; i++)
    	temp = (temp << 8) | (data[i] & 0xff);
    x2 = Double.longBitsToDouble(temp);
    temp = 0;
    for (int i = 24; i < 32; i++)
    	temp = (temp << 8) | (data[i] & 0xff);
    y2 = Double.longBitsToDouble(temp);
    index = (index < textData.length) ? index + 1 : index; 
    System.arraycopy(textData, index, textData, 0, text.getLength() - index);
    text.set(textData, 0, text.getLength() - index);
    tags = new String(text.getBytes());
  }

  @Override
  public String toString() {
    return "#" + nodeId + ", " + "#" + tableId + super.toString() + ", " + tags;
  }
}
