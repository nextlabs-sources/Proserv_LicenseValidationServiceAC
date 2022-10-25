package com.nextlabs.spiritaero;

import java.io.File;
import java.io.FileInputStream;
import java.net.URLDecoder;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class PropertyLoader
{
    
    public static Properties loadPropertiesInPDP(String filePath) {

		// get the properties file
		/*
		 * String dpcInstallHome = System.getProperty("dpc.install.home"); if
		 * (dpcInstallHome == null || dpcInstallHome.trim().length() < 1) {
		 * dpcInstallHome = "."; }
		 * 
		 * LOG.info("DPC Install Home :" + dpcInstallHome);
		 * 
		 * filePath = dpcInstallHome + filePath;
		 */

		String installLoc = findInstallFolder();

		LOG.info(String.format("Install location is: %s", installLoc));

		if (filePath == null) {	
			LOG.error("File path is undefined");
		}

		filePath = installLoc + filePath;

		if (filePath.startsWith("/")) {
			filePath = "." + filePath;
		}

		LOG.info("Properties File Path:: " + filePath);

		Properties result = null;

		try {
			File file = new File(filePath);

			if (file != null) {
				FileInputStream fis = new FileInputStream(file);
				result = new Properties();
				result.load(fis); // Can throw IOException
			}
		} catch (Exception e) {
			LOG.error("Error parsing properties file ", e);
			result = null;
		}
		return result;
	}
    
    public static String findInstallFolder() {

		String path = PropertyLoader.class.getProtectionDomain().getCodeSource().getLocation().getPath();

		try {
			path = URLDecoder.decode(path, "UTF-8");

		} catch (Exception e) {
			LOG.error(String.format("Exception while decoding the path: %s", path), e);
		}

		int endIndex = path.indexOf("jservice/jar");

		if (isWindows()) {
			path = path.substring(1, endIndex);
		} else {
			path = path.substring(0, endIndex);
		}
		return path;
	}
	
	public static String getOsName() {
		if (OS == null) {
			OS = System.getProperty("os.name");
		}
		return OS;
	}

	public static boolean isWindows() {
		return getOsName().startsWith("Windows");
	}
        
    private static final Log LOG = LogFactory.getLog(PropertyLoader.class);
	private static String OS = null;
} // End of class