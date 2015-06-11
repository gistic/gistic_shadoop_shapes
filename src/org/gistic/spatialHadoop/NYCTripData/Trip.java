package org.gistic.spatialHadoop.NYCTripData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * @author kareem
 */
public class Trip extends Point {

    private String attributes;    

    public Trip() {}

    public Trip(double x, double y) {
        super(x, y);
    }

    public Trip(Trip trip) {
        this.attributes = trip.attributes;
        this.x = trip.x;
        this.y = trip.y;
    }

    @Override
    public void fromText(Text text) {
        /* 
            columnNames : 
               
                medallion,
                hack_license,
                vendor_id,
                rate_code,
                store_and_fwd_flag,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_time_in_secs,
                trip_distance,
                pickup_longitude,
                pickup_latitude,
                dropoff_longitude,
                dropoff_latitude

            Sample Values :

                 89D227B655E5C82AECF13C3F540D4CF4,
                 BA96DE419E711691B9445D6A6307C170,
                 CMT,
                 1,
                 N,
                 2013-01-01 15:11:48,
                 2013-01-01 15:18:10,
                 4,
                 382,
                 1.00,
                 -73.978165,
                 40.757977,
                 -73.989838,
                 40.751171
        */

        String row = text.toString();

        String[] fields = row.split(",", -1);
             
        try {

            String pickupLongitude = fields[10];
            String pickupLatitude = fields[11];
            String dropoffLongitude = fields[12];
            String dropoffLatitude = fields[13];

            this.x = Double.parseDouble(pickupLongitude);
            this.y = Double.parseDouble(pickupLatitude);
            this.attributes = text.toString();
            
        } catch (Exception e){
            // First reasonable number
            this.x = -73.989838;
            this.y = 40.751171;
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
    public Trip clone() {
        return new Trip(this);
    }
}
