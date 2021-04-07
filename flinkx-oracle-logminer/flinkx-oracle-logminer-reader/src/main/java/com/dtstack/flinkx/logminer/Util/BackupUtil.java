package com.dtstack.flinkx.logminer.Util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;

public class BackupUtil {
	
	private static Logger LOG = LoggerFactory.getLogger(BackupUtil.class);

	/**
	 * 更新本地任务的备份文件
	 * @param
	 */
	public static void updateLocalBackup(String id,Long scn) {
		String fileUrl = getFileUrl(id);
		File file = new File(fileUrl);
		if(!file.getParentFile().exists()) {
			file.getParentFile().mkdirs();
		}
		if(!writeFileText(fileUrl, scn.toString(), false)) {
			LOG.error("update local backup fail, please check this file:{}",fileUrl);
		}
	}

	/**
	 *  获取本地任务的备份文件
	 * @param
	 */
	public static Long getLocalJobBackup(String id) {
		String fileUrl = getFileUrl(id);
		File backupFile = new File(fileUrl);
		if(!backupFile.exists()) {
			LOG.info("local backup isn't exist, please check this file:{}",fileUrl);
			return null;
		}
		String content = getFileText(fileUrl);
		if(StringUtils.isBlank(content)) {
			LOG.info("local backup is blank, please check this file:{}",fileUrl);
			return null;
		}
		return Long.valueOf(content);
	}

	private static String getFileUrl(String id) {
		return "./scn/"+id+"/scn.txt";
	}

	public static boolean writeFileText(String fileName, String content, boolean append)
	{
		FileWriter writer = null;

		try
		{
			writer = new FileWriter(fileName, append);
			writer.write(content);
			writer.flush();

			return true;
		}
		catch (Exception err)
		{
			LOG.error(fileName, err);
			return false;
		}
		finally
		{
			try
			{
				if (writer != null)
					writer.close();
			}
			catch (Exception err)
			{
			}
		}
	}

	public static String getFileText(String fileName)
	{
		InputStream input = null;
		try
		{
			input = getFileInputStream(fileName);
			String content = IOUtils.toString(input);

			return content;
		}
		catch (Throwable err)
		{
			LOG.warn("Read file {} failed.", fileName);
			if (LOG.isDebugEnabled())
				LOG.error(null, err);
			return "";
		}
		finally
		{
			IOUtils.closeQuietly(input);
		}
	}

	public static InputStream getFileInputStream(String fileName)
	{
		try
		{
			return new FileInputStream(fileName);
		}
		catch (Exception err)
		{
			throw new RuntimeException(err);
		}
	}


}
