package com.dtstack.flinkx.pg9wal;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.util.LongValueSequenceIterator;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import com.dtstack.flinkx.util.DateUtil;

public class test2 {

	public static void main(String[] args) throws SQLException, InterruptedException {
		Connection con = null;
		PGConnection replConnection ;
		try{
//			String url = "jdbc:postgresql://localhost:5432/postgres";
//		    Properties props = new Properties();
//		    PGProperty.USER.set(props, "postgres");
//		    PGProperty.PASSWORD.set(props, "linkcm12306");
		String url = "jdbc:postgresql://120.78.216.6:11085/postgres";
	    Properties props = new Properties();
	    PGProperty.USER.set(props, "postgres");
	    PGProperty.PASSWORD.set(props, "postgres");
	    PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
	    PGProperty.REPLICATION.set(props, "database");
	    PGProperty.PREFER_QUERY_MODE.set(props, "simple");

	    con = DriverManager.getConnection(url, props);
	    replConnection = con.unwrap(PGConnection.class);

//	    replConnection.getReplicationAPI()
//	        .createReplicationSlot()
//	        .logical()
//	        .withSlotName("ali_logical_slot")
////	        .withOutputPlugin("wal2json")
//	        .withOutputPlugin("ali_decoding")
//	        .make();
////	    
//	    LogSequenceNumber waitLSN = LogSequenceNumber.valueOf("0/1640948");
//	    System.out.println(waitLSN.asLong());
	    PGReplicationStream stream =
	        replConnection.getReplicationAPI()
	            .replicationStream()
	            .logical()
//	            .physical()
//	            .withStartPosition(waitLSN)
//	            .withSlotName("ali_logical_slot")
//	            .withSlotOption("include-xids", true)
//	            .withSlotOption("include-timestamp", true)
//	            .withSlotOption("include-rewrites", true)
//	            .withSlotOption("stream-changes", true)
//	            .withSlotOption("skip-empty-xacts", false)
	            .withStatusInterval(20, TimeUnit.MICROSECONDS)
	            .start();
	    stream.forceUpdateStatus();
	    System.out.println("LastReceiveLSN:"+stream.getLastReceiveLSN().asLong());
	    stream.setAppliedLSN(stream.getLastReceiveLSN());
    	stream.setFlushedLSN(stream.getLastReceiveLSN());
	    Boolean bool = false;
	    while (true) {
	      //non blocking receive message
//	    	location  | xid |                       data
//	    	-----------+-----+--------------------------------------------------
//	    	 0/16D30F8 | 691 | BEGIN
//	    	 0/16D32A0 | 691 | table public.data: INSERT: id[int4]:2 data[text]:'arg'
//	    	 0/16D32A0 | 691 | table public.data: INSERT: id[int4]:3 data[text]:'demo'
//	    	 0/16D32A0 | 691 | COMMIT
	      ByteBuffer msg = stream.readPending();

	      if (msg == null) {
	        TimeUnit.MILLISECONDS.sleep(20L);
	        continue;
	      }
	   
	      int offset = msg.arrayOffset();
	      byte[] source = msg.array();
	      int length = source.length - offset;
	      System.out.println(new String(source, offset, length));

	      System.out.println("LastReceiveLSN:"+stream.getLastReceiveLSN().asLong());
		    
	      //feedback
	      stream.setAppliedLSN(stream.getLastReceiveLSN());
	      stream.setFlushedLSN(stream.getLastReceiveLSN());
	    }
		}catch (Exception e) {
			e.printStackTrace();
			if(con!= null) {
				con.close();
			}
		}
	    
	}
	
	
	public static void main1(String[] args) {
		byte bys[] = new byte[]{Byte.decode("0x00"),Byte.decode("10100100")};
		System.out.println(new String(bys));
	}
}
