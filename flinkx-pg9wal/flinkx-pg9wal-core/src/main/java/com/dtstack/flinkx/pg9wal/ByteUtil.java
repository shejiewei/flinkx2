package com.dtstack.flinkx.pg9wal;

public class ByteUtil {

	static double min[] = { 187.0, 187.0, 188.0, 233.0, 0.00 };
	static double max[] = { 315.0, 315.0, 273.0, 318.0, 1.00 };

	public static int bt2hd(double bt, int band) {
		int _hd = 0;
		if (band < 4)
			_hd = (int) (255 * (bt - max[band]) / (min[band] - max[band]));
		else
			_hd = (int) (255 * (bt - min[band]) / (max[band] - min[band]));
		if (_hd < 0)
			_hd = 0;
		if (_hd > 255)
			_hd = 255;
		return (_hd);
	}

	public static short bytesToShort(byte[] b) {
		return (short) (b[0] & 0xff | (b[1] & 0xff) << 8);
	}
	
	public static int byteArrayToInt(byte[] bytes) {  
	    int value = 0;  
	    // 由高位到低位  
	    for (int i = 0; i < 4; i++) {  
	        int shift = (4 - 1 - i) * 8;  
	        value += (bytes[i] & 0x000000FF) << shift;// 往高位游  
	    }  
	    return value;  
	}  
	
	/**
	 * byte转换成short
	 * 
	 * @param b
	 * @return
	 */
	public static short byteToShort(byte[] b) {
		short s = 0;
		short s0 = (short) (b[0] & 0xff);
		short s1 = (short) (b[1] & 0xff);
		s1 <<= 8;
		s = (short) ((s0 | s1) & 0xffff);
		return s;
	}

	public static int bytes2int(byte[] b) {
		int mask = 0xff;
		int temp = 0;
		int res = 0;
		for (int i = 0; i < 4; i++) {
			res <<= 8;
			temp = b[i] & mask;
			res |= temp;
		}
		return res;
	}

	// 注意高低位问题
	public static int bytesToInt(byte[] src, int offset) {
		int value;
		value = (int) ((src[offset] & 0xFF) | ((src[offset + 1] & 0xFF) << 8) | ((src[offset + 2] & 0xFF) << 16) | ((src[offset + 3] & 0xFF) << 24));
		return value;
	}

	public static int bytes2int2(byte[] b) {
		int mask = 0xff;
		int temp = 0;
		int res = 0;
		for (int i = 3; i >= 0; i--) {
			res <<= 8;
			temp = b[i] & mask;
			res |= temp;
		}
		return res;
	}

	// byte数组转成long
	public static long byteToLong(byte[] b) {
		long s = 0;
		long s0 = b[0] & 0xff;// 最低位
		long s1 = b[1] & 0xff;
		long s2 = b[2] & 0xff;
		long s3 = b[3] & 0xff;
		long s4 = b[4] & 0xff;// 最低位
		long s5 = b[5] & 0xff;
		long s6 = b[6] & 0xff;
		long s7 = b[7] & 0xff;

		// s0不变
		s1 <<= 8;
		s2 <<= 16;
		s3 <<= 24;
		s4 <<= 8 * 4;
		s5 <<= 8 * 5;
		s6 <<= 8 * 6;
		s7 <<= 8 * 7;
		s = s0 | s1 | s2 | s3 | s4 | s5 | s6 | s7;
		return s;
	}

	/**
	 * 字节转换为浮点
	 * 
	 * @param b
	 *            字节（至少4个字节）
	 * @param index
	 *            开始位置
	 * @return
	 */
	public static float byte2float(byte[] b, int index) {
		int l;
		l = b[index + 0];
		l &= 0xff;
		l |= ((long) b[index + 1] << 8);
		l &= 0xffff;
		l |= ((long) b[index + 2] << 16);
		l &= 0xffffff;
		l |= ((long) b[index + 3] << 24);
		return Float.intBitsToFloat(l);
	}

	/**
	 * 字节转换为浮点
	 * 
	 * @param arr
	 *            字节数组（至少4个字节）
	 * @return
	 */
	public static double byte2double(byte[] arr) {
		long value = 0;
		for (int i = 0; i < 8; i++) {
			value |= ((long) (arr[i] & 0xff)) << (8 * i);
		}

		return Double.longBitsToDouble(value);
	}

	/**
	 * 将字节数组转换为String
	 * 
	 * @param b
	 *            byte[]
	 * @return String
	 */
	public static String bytesToString(byte[] b) {
		StringBuffer result = new StringBuffer("");
		int length = b.length;
		for (int i = 0; i < length; i++) {
			result.append((char) (b[i]));
		}
		return result.toString();
	}

}
