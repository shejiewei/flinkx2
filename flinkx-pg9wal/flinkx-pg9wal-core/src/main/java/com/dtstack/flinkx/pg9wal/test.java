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

public class test {

	public static void main(String[] args) throws SQLException, InterruptedException {
		Connection con = null;
		PGConnection replConnection ;
		try{
//		String url = "jdbc:postgresql://localhost:5432/postgres";
//	    Properties props = new Properties();
//	    PGProperty.USER.set(props, "postgres");
//	    PGProperty.PASSWORD.set(props, "linkcm12306");
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
//	        .withSlotName("demo_logical_slot1")
//	        .withOutputPlugin("pgoutput")
//	        .withOutputPlugin("test_decoding")
//	        .make();
	    
//	    replConnection.getReplicationAPI()
//        .createReplicationSlot()
//        .physical()
//        .withSlotName("demo_logical_slot2")
//        .withOutputPlugin("pgoutput")
//        .withOutputPlugin("test_decoding")
//        .make();

	    //some changes after create replication slot to demonstrate receive it
	    /*sqlConnection.setAutoCommit(true);
	    Statement st = sqlConnection.createStatement();
	    st.execute("insert into test_logic_table(name) values('first tx changes')");
	    st.close();

	    st = sqlConnection.createStatement();
	    st.execute("update test_logic_table set name = 'second tx change' where pk = 1");
	    st.close();

	    st = sqlConnection.createStatement();
	    st.execute("delete from test_logic_table where pk = 1");
	    st.close();*/
	    LogSequenceNumber waitLSN = LogSequenceNumber.valueOf("0/8029820");
//	    System.out.println(waitLSN.asLong());
	    PGReplicationStream stream =
	        replConnection.getReplicationAPI()
	            .replicationStream()
	            .logical()
//	            .physical()
	            .withStartPosition(waitLSN)
	            .withSlotName("demo_logical_slot2")
//	            .withSlotOption("include-xids", true)
//	            .withSlotOption("include-timestamp", true)
//	            .withSlotOption("include-rewrites", true)
//	            .withSlotOption("stream-changes", true)
//	            .withSlotOption("skip-empty-xacts", false)
	            .withStatusInterval(20, TimeUnit.MICROSECONDS)
	            .start();
	    stream.forceUpdateStatus();
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
	      
//	      System.out.println(new Date().getTime());
//	      byte type = msg.get();
//	      System.out.println((char)type );
//	      while(msg.hasRemaining()) {
//	    	  System.out.print(msg.getInt() );
//	      }
	    byte length1[] = new byte[1];
		byte length2[] = new byte[2];
		byte length3[] = new byte[3];
		byte length4[] = new byte[4];
		byte length5[] = new byte[5];
		byte length8[] = new byte[8];
		byte length32[] = new byte[32];
		byte length56[] = new byte[56];
//		if(!bool) {
//			  msg.get(length2,0,2) ;
//		      System.out.println(ByteUtil.bytesToShort(length2));
//		      msg.get(length2,0,2) ;
//		      System.out.println(ByteUtil.bytesToShort(length2) );
//		      msg.get(length4,0,4) ;
//		      System.out.println(ByteUtil.bytesToInt(length4,0) );//length
//		      msg.get(length8,0,8) ;
//		      System.out.println(ByteUtil.byteToLong(length8) );//length
//		      msg.get(length4,0,4) ;
//		      System.out.println(ByteUtil.bytesToInt(length4,0) );//length
//	          msg.get(length8,0,8) ;
//		      System.out.println(ByteUtil.bytesToString(length8) );//length
//		      msg.get(length4,0,4) ;
//		      System.out.println(ByteUtil.bytesToInt(length4,0) );//length
//		      msg.get(length4,0,4) ;
//		      System.out.println(ByteUtil.bytesToInt(length4,0) );//length
//		      bool = true;
//	      }
		
//	      System.out.println();
//	      msg.position(8);
//	      msg.get(length8) ;
//	      System.out.println("本页xlog地址:" +ByteUtil.byteToLong(length8) );
//	      msg.get(length4) ;
//	      System.out.println("remain data 长度:" +ByteUtil.bytesToInt(length4,0) );
//	      msg.position(28);
//	      msg.get(length4) ;
//	      System.out.println("segment 长度:" +ByteUtil.bytesToInt(length4,0) );
//	      msg.get(length4) ;
//	      System.out.println("page 长度:" +ByteUtil.bytesToInt(length4,0) );
		System.out.println("array 总长度："+msg.array().length);
	      msg.get(length4) ;
	      int leng = ByteUtil.bytesToInt(length4,0);
	      System.out.println("xlog总长度:" + leng);
	      msg.get(length4) ;
	      System.out.println("事务Id:" +ByteUtil.bytesToInt(length4,0) );
	      msg.get(length8) ;
//	      System.out.println("前一个记录位置:" +ByteUtil.byteToLong(length8) );
//	      msg.get(length8) ;
	      int childType = (msg.get() & 0Xf0) /16;
	      
	      int resourceType = (msg.get() & 0X0f) ;
	      msg.position(24);
	      //blockheader
	      if(resourceType == 10 && childType <= 4 && childType != 3) {
	    	  System.out.println("记录子类型:" + childType);
		      System.out.println("记录类型:" +resourceType);
	    	  System.out.println("数据库操作记录:--------------------------" );
	    	  System.out.println("blockheader position:" + msg.position());
	    	  int blockNum = msg.get() & 0Xff;
		      System.out.println("block id:" +blockNum);
		      byte flag = msg.get() ;
		      int blockType = flag & 0X0f;
		      System.out.println("block存储类型:" +blockType);
//		      #define BKPBLOCK_FORK_MASK  0x0F
//		      #define BKPBLOCK_FLAG_MASK  0xF0
//		      #define BKPBLOCK_HAS_IMAGE  0x10    /* block data is an XLogRecordBlockImage */
//		      #define BKPBLOCK_HAS_DATA   0x20
//		      #define BKPBLOCK_WILL_INIT  0x40    /* redo will re-init the page */
//		      #define BKPBLOCK_SAME_REL   0x80
		      int HAS_IMAGE = (flag & 0x10 )/16;
		      System.out.println("block是否有快照:" +HAS_IMAGE);
		      int HAS_DATA = (flag & 0x20 )/32;
		      System.out.println("block是否有数据:" +HAS_DATA);
		      int WILL_INIT = (flag & 0x40 )/64;
		      System.out.println("block是否需要重新初始化本页:" +WILL_INIT);
		      int SAME_REL = (flag & 0x80 )/128;
		      System.out.println("SAME_REL:" +SAME_REL);
	    	  msg.get(length2) ;
	    	  int tuple = ByteUtil.byteToShort(length2);
		      System.out.println("tuple size:" +tuple );
//		      msg.get(length5) ;
		      
//		      int offset = msg.arrayOffset();
//		      byte[] source = msg.array();
//		      int length = source.length - offset;
//		      System.out.println(new String(source, offset, length));
		      int pageSize = 0;
		      //blockimageheader
		      if(HAS_IMAGE == 1) {

		    	  System.out.println("-----------blockimageheader position:" + msg.position());
		    	  msg.get(length2) ;
			      pageSize = ByteUtil.byteToShort(length2);
			      System.out.println("page size:" + pageSize);
			      msg.get(length2) ; // hole_offset
			      msg.get(length1) ; //flag compress
//			      System.out.println("事务Id:" +ByteUtil.bytesToInt(length2,0) );
			      
		      }
		      
		      if(SAME_REL == 0) {
		    	  System.out.println("-----------RelFileNode position:" + msg.position());
//		    	  msg.position(msg.position()+12);
		    	  msg.get(length4) ;
		    	  System.out.println("tablespace:"+ByteUtil.bytesToInt(length4,0));
		    	  msg.get(length4) ;
		    	  System.out.println("database node:"+ByteUtil.bytesToInt(length4,0));
		    	  msg.get(length4) ;
		    	  System.out.println("relation node:"+ByteUtil.bytesToInt(length4,0));
			      
		      }
		      
		      System.out.println("-----------BlockNumber position:" + msg.position());
		      msg.get(length4) ;
	    	  System.out.println("BlockNumber:"+ByteUtil.bytesToInt(length4,0));
		      
		      
		      System.out.println("-----------record header position:" + msg.position());
		      System.out.println("id:" + (msg.get() & 0xff));
		      System.out.println("main data size:" + (msg.get() & 0xff));
//		      msg.position(46);
		      
		      if(HAS_IMAGE == 1 && pageSize != 0) {
		    	  System.out.println("------------- page position:" + msg.position());
		    	 
		    	  msg.position(msg.position()+pageSize);
		      }
			      if( HAS_DATA == 1) {
			    	  System.out.println("-------------heap header position:" + msg.position());
			    	  msg.get(length2) ;
			    	  System.out.println("info mask1:"+ByteUtil.byteToShort(length2));
				      msg.get(length2) ;
				      System.out.println("info mask2:"+ByteUtil.byteToShort(length2));
				      System.out.println("hoff:" + (msg.get() & 0xff));
			      }  
			    	  int tupleP = msg.position();
			    	  System.out.println("-------------tuple position:" + msg.position());
			    	  int mianP = 0;
			    	  if(childType == 0) {
			    		  mianP = 3;
			    		  msg.limit();
				    	  msg.position(leng -3); 
//				    	  msg.position(msg.limit() -3); 
				    	  
				    	  System.out.println("-------------main data position:" + msg.position());
				    	  msg.get(length2);
				    	  short offset1 = ByteUtil.byteToShort(length2);
				    	  System.out.println("insert offset :" + offset1);
				    	  
				    	  byte inflag = msg.get();
				    	  System.out.println("XLH_INSERT_CONTAINS_NEW_TUPLE  :" + (inflag & 0x08)/8);
				    	  
				    	  msg.position(leng -3); 
				    	  msg.get(length3);
				    	  System.out.println("main data HEX :" + Hex.encodeHexString(length3));
					      
			    	  }else if(childType == 1) {
			    		  mianP = 8;
			    		  msg.position(leng -8); 
//			    		  msg.position(msg.limit() -8); 
				    	  
				    	  System.out.println("-------------main data position:" + msg.position());
				    	  msg.get(length4);
				    	  short otid = ByteUtil.byteToShort(length4);
				    	  System.out.println("old transection id  :" + otid);
				    	  
				    	  msg.get(length2);
				    	  short offset1 = ByteUtil.byteToShort(length2);
				    	  System.out.println("offset  :" + offset1);
				    	  
				    	  msg.get();
				    	  byte inflag = msg.get();
				    	  System.out.println("XLH_INSERT_CONTAINS_NEW_TUPLE  :" + (inflag & 0x08)/8);
			    	  
			    	  }else if(childType == 4 || childType == 2) {
			    		  mianP = 14;
			    		  msg.position(leng -16); 

//			    		  msg.position(msg.limit() -14); 
				    	  
			    		  System.out.println("-------------main data position:" + msg.position());
				    	  msg.get(length4);
				    	  short otid = ByteUtil.byteToShort(length4);
				    	  System.out.println("old transection id  :" + otid);
				    	  
				    	  msg.get(length2);
				    	  short offset1 = ByteUtil.byteToShort(length2);
				    	  System.out.println("old offset  :" + offset1);
				    	  
				    	  msg.get();
				    	  byte inflag = msg.get();
				    	  System.out.println("XLH_INSERT_CONTAINS_NEW_TUPLE  :" + (inflag & 0x08)/8);

				    	  msg.get(length4);
				    	  int ntid = ByteUtil.bytesToInt(length4,0);
				    	  System.out.println(" transection id  :" + ntid);
				    	  
				    	  msg.get(length2);
				    	  short noffset = ByteUtil.byteToShort(length2);
				    	  System.out.println("offset  :" + noffset);
				    	  
			    	  }
			    	  
			    	  msg.position(tupleP);
			    	  if(msg.hasArray() && HAS_DATA == 1) {
			    		  int offset = msg.arrayOffset();
					      byte[] source = msg.array();
					      int length = source.length ;
					      System.out.println("array size:" + length);
					      System.out.println(new String(source, tupleP, tuple-5));
					      System.out.println("HEX :" + Hex.encodeHexString(Arrays.copyOfRange(source, tupleP, tupleP+tuple-5)));
			    	  }
			    	  
			    	  
			    	  if(msg.hasArray()) {
			    		  
			    		  int offset = msg.arrayOffset();
					      byte[] source = msg.array();
					      int length = source.length ;
//					      System.out.println("array size:" + length);
					      System.out.println(new String(source, tupleP, length-tupleP,"GBK"));
					      System.out.println("test main data HEX :" + Hex.encodeHexString(Arrays.copyOfRange(source, length -mianP , length)));
					      System.out.println("main data HEX :" + Hex.encodeHexString(Arrays.copyOfRange(source, leng -mianP , leng)));
					      System.out.println("HEX :" + Hex.encodeHexString(Arrays.copyOfRange(source, tupleP, length)));
			    	  }
			    	  
			    	  msg.position(leng);
			      }
	      System.out.println();
	      
//	      msg.get(length56,0,56) ;
//	      System.out.println(msg.getLong());
//	      System.out.println(ByteUtil.bytesToInt(length4,0) );//length
//	      msg.get(length4,0,4) ;
//	      System.out.println(ByteUtil.bytesToInt(length4,0) );//length
//	      msg.get(length4,0,4) ;
//	      System.out.println(ByteUtil.bytesToInt(length4,0) );//length
//	      msg.get(length4,0,4) ;
//	      System.out.println(msg.getChar() );//length
//	      System.out.println(msg.getChar() );//info
	      
//	      System.out.println(msg.getLong(32) );
	      
//	      System.out.println(msg );
	      
//	      int offset = msg.arrayOffset();
//	      byte[] source = msg.array();
//	      int length = source.length - offset;
//	      System.out.println(new String(source, offset, length));

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
