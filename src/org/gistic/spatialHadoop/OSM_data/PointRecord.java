package org.gistic.spatialHadoop.OSM_data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * @author kareem
 */
public class PointRecord extends Point {

    private String attributes;    

    public PointRecord() {}

    public PointRecord(double x, double y) {
        super(x, y);
    }

    public PointRecord(PointRecord PointRecord) {
        this.attributes = PointRecord.attributes;
        this.x = PointRecord.x;
        this.y = PointRecord.y;
    }

    @Override
    public void fromText(Text text) {
        /* 
            columnNames : 
               
                node_id
                longitude
                latitude
                tags

            Example :

                11  73.5122471  4.0838053   [name#Embudu,is_in#Maldives,place#island]

        */

        String row = text.toString();

        String[] fields = row.split("\\s", -1);
             
        try {

            String nodeID = fields[0];
            String longitude = fields[1];
            String latitude = fields[2];
            String tags = fields[3];

            this.x = Double.parseDouble(longitude);
            this.y = Double.parseDouble(latitude);
            this.attributes = text.toString();
            
        } catch (Exception e){
            // First reasonable number
            this.x = -1000000;
            this.y = 1000000;
            this.attributes = text.toString();

            System.out.println("======= Exception reading object ");
            System.out.println(this.attributes);
        }
        
    }

    @Override
    public Text toText(Text text) {        
        text.append(attributes.getBytes(), 0, attributes.getBytes().length);
        return text;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(x);
        out.writeDouble(y);
        out.writeUTF(attributes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.x = in.readDouble();
        this.y = in.readDouble();
        this.attributes = in.readUTF();
    }

    @Override
    public PointRecord clone() {
        return new PointRecord(this);
    }
}
