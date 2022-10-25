package com.nextlabs.spiritaero;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import com.bluejungle.framework.expressions.EvalValue;
import com.bluejungle.framework.expressions.IEvalValue;
import com.bluejungle.framework.expressions.IMultivalue;
import com.bluejungle.framework.expressions.Multivalue;
import com.bluejungle.framework.expressions.ValueType;
import com.bluejungle.pf.domain.destiny.serviceprovider.IFunctionServiceProvider;
import com.bluejungle.pf.domain.destiny.serviceprovider.ServiceProviderException;
import com.nextlabs.spiritaero.PropertyLoader;

public class LicenseValidationService implements IFunctionServiceProvider {

	private static final Log LOG = LogFactory.getLog(LicenseValidationService.class);
	
	private static DataSource datasource;	
	private static String userAttrQuery;
	private static String typeIsEmployee;
	private static Properties prop;
	
	/*
	 * (non-Javadoc)
	 * @see com.bluejungle.pf.domain.destiny.serviceprovider.IServiceProvider#init()
	 */
	public void init() throws Exception {
		
		LOG.info("init LicenseValidationService");

		prop = PropertyLoader.loadPropertiesInPDP("/jservice/config/LicenseValidationService.properties");
		
		if (null!=prop){
			
			String sDBUrl = prop.getProperty("db-url");
			String sDBUser = prop.getProperty("db-user");
			String sDBPassword = prop.getProperty("db-password");				
			datasource = setupConnectionPool(sDBUrl, sDBUser, sDBPassword);
			
			// Sandeep says: keep the query configurable
			
			// Current: userAttrQuery = "SELECT VPG.EMPLID AS EMPLID, VPG.PRF_USERID AS PRF_USERID, VPN.CNTRY_OF_CIT_NM AS CNTRY_OF_CIT_NM , VPN.CNTRY_OF_CIT_NM2 AS CNTRY_OF_CIT_NM2, VPG.EXT_CO_NM AS EXT_CO_NM, VPG.SITE_NM AS SITE_NM, VPG.RLTNP_CD AS RLTNP_CD FROM DBADMIN.V_NEXTLABS_ACTIVE_USERS VNAU, DBADMIN.V_PEOPLE_NEXTLABS@CED VPN, DBADMIN.V_PEOPLE_GENERAL@CED VPG WHERE VPN.EMPLID = VPG.EMPLID AND VNAU.EMPLID = VPN.EMPLID AND VNAU.ACCOUNTNAME = ";
			userAttrQuery = prop.getProperty("user-attr-query");  
			// Old for reference userAttrQuery = "SELECT VPG.EMPLID AS EMPLID, VPG.PRF_USERID AS PRF_USERID, VPN.CNTRY_OF_CIT_NM AS CNTRY_OF_CIT_NM , VPN.CNTRY_OF_CIT_NM2 AS CNTRY_OF_CIT_NM2, VPG.EXT_CO_NM AS EXT_CO_NM, VPG.SITE_NM AS SITE_NM, VPG.RLTNP_CD AS RLTNP_CD FROM DBADMIN.V_PEOPLE_NEXTLABS@CED VPN, DBADMIN.V_PEOPLE_GENERAL@CED VPG WHERE VPN.EMPLID = VPG.EMPLID AND VPN.EMPLID = ";
					    
			// Sandeep says: keep the query configurable
			typeIsEmployee = prop.getProperty("type-is-employee"); // e.g. "employee", "contractor", "service provider",
			// typeIsEmployee = "EMPLOYEE";
		}
		
		LOG.info("Completed init LicenseValidationService");
	}

	/**
	 * This function checks the record in the database based on the input parameter
	 * 
	 */
	public IEvalValue callFunction(String functionName, IEvalValue[] args)
			throws ServiceProviderException {

		LOG.info("Start LicenseValidationService");

		try{

			/* Check the access to EAR documents */
			if ("ValidateUserAccessToEARData".equalsIgnoreCase(functionName)) {

				long lCurrentTime = System.nanoTime();
				LOG.info("callFunction ValidateUserAccessToEARData() called");

				if (args.length < 3){
					LOG.error("Parameters pass in should be more or equals to 3");
					return EvalValue.build("no");
				}

				if (ValidateUserAccessToEARData(args)) {
					LOG.info("Return yes. Time spent for AC = "+ ((System.nanoTime() - lCurrentTime)/1000000.00) +"ms");
					return EvalValue.build("yes");
				} else {
					LOG.info("Return no. Time spent for AC = "+ ((System.nanoTime() - lCurrentTime)/1000000.00) +"ms");
					return EvalValue.build("no");
				}
			} else if ("ValidateUserAccessByLocationOEM".equalsIgnoreCase(functionName)) {

				long lCurrentTime = System.nanoTime();
				LOG.info("callFunction ValidateUserAccessByLocationOEM() called");

				if (args.length < 3){

					LOG.error("Parameters pass in should be more or equals to 3");
					return EvalValue.build("no");
				}

				if (ValidateUserAccessByLocationOEM(args)) {
					LOG.info("Return yes. Time spent for AC = "+ ((System.nanoTime() - lCurrentTime)/1000000.00) +"ms");
					return EvalValue.build("yes");
				} else {
					LOG.info("Return no. Time spent for AC = "+ ((System.nanoTime() - lCurrentTime)/1000000.00) +"ms");
					return EvalValue.build("no");
				}
			}

		}catch(Exception ex){
			LOG.error(ex.toString());
		}

		//Default to return no
		LOG.error("End LicenseValidationService and return no");
		return EvalValue.build("no");
	}
	
	/**
	 * Process the input data and put in arraylist
	 * @param args IEvalValue list that pass in from Policy Controller
	 * @return Arraylist of string that contain data
	 */
	private ArrayList<String> processValues(IEvalValue[] args){

		int i=0;

		ArrayList<String> sOutData = new ArrayList<String>();

		for (IEvalValue ieValue: args){

			String sData = "";

			if (null!=ieValue){

				if (ieValue.getType() == ValueType.MULTIVAL) {
					IMultivalue mv = (IMultivalue) ieValue.getValue();

					for (Iterator<IEvalValue> it = mv.iterator(); it.hasNext();) {
						IEvalValue ev = it.next();
						if (ev.getType() == ValueType.STRING) {
							if (!ev.getValue().toString().isEmpty()) {
								sData = sData + "," + ev.getValue().toString();
							}
						}
					}

				} else if (ieValue.getType() == ValueType.STRING) {
					sData = ieValue.getValue().toString();
				}
				
				LOG.info("----"+i + "." + sData +"-----");
				
				//Filter out the empty string data
				if (sData.length()>0)
					sOutData.add(sData);
			}

			i++;
		}

		return sOutData;

	}
	
//	/**
//	 * Convert StringArray to format that can be use in SQL query "in" e.g from ,a,b,c to 'a','b','c'
//	 * @param sDatas Array of string
//	 * @return converted data
//	 */
//	private String convertValues2InFormat(String sDatas[]){
//		
//		String sResult = "";
//		int i =0;
//		
//		for (String sData:sDatas){
//			
//			//Discard the empty string
//			if(sData.length()<1){
//				continue;
//			}
//			
//			if (i==0){
//				sResult = "'" + sData + "',";
//			}
//			else{
//				sResult = sResult + "'" + sData + "',";
//			}
//			
//			i++;		
//		}
//		
//		//Remove the last character which is ,
//		return sResult.substring(0,sResult.length()-1);
//	}

	
	/**
	 * Getting the connection from database and checking if database have the record
	 * @param sColumn IEvalValue list from policy controller
	 * @return true if have record and false if not record
	 */
	private boolean ValidateUserAccessToEARData(IEvalValue[] sColumn) {
		
		ArrayList<String> sArrDataInput = processValues(sColumn);
		
		if (sArrDataInput.size()<3){
			LOG.error("Input data is invalid, return false");
			return false;
		}
		
		LOG.info("Checking if license is required.");
		
		String sAuthCode = getLicenseRequired(sArrDataInput.get(1),sArrDataInput.get(2));
		
		if (sAuthCode.equals("-1")){ 
			//Cannot determine lic check Y/N - error case
			return false;
		}
		else if (sAuthCode.equals("-2")){
			//NLR case
			ArrayList<String> sUserInfo = getUserAttributes(sArrDataInput.get(0));
			
			if(sUserInfo!=null){
				return true;
			}
			return false;
		} else {
			
			ArrayList<String> sUserInfo = getUserAttributes(sArrDataInput.get(0));
			
			if(sUserInfo!=null){
				String sLicenseCode = getEARLicenseCode(sUserInfo.get(2), sUserInfo.get(3),sUserInfo.get(1), sArrDataInput.get(1));
				if (!sLicenseCode.equals("")){
						return true;
				}
			}
		}
		
		return false;
	}
	
	/**
	 * Validate user has access to OEM documents for her work location
	 * @param sColumn IEvalValue list from policy controller
	 * @return true if have record and false if not record
	 */
	private boolean ValidateUserAccessByLocationOEM(IEvalValue[] sColumn) {
		
		ArrayList<String> sArrDataInput = processValues(sColumn);
		
		if (sArrDataInput.size()<3){
			LOG.error("Input data is invalid, return false");
			return false;
		}

		ArrayList<String> sUserInfo = getUserAttributes(sArrDataInput.get(0));
		
		if (sUserInfo==null){
			return false;
		}
		
		String sAuthCode = sArrDataInput.get(2);
		
		if (getOEMListByLocation(sUserInfo.get(4), sAuthCode)){
			return true;
		}
		
		return false;		
	}
	
	
	
	/**
	 * Get user information from table NXL_USER
	 * @param sUserId
	 * @return
	 */
	private ArrayList<String> getUserAttributes(String sUserId){
		
		long lCurrentTime = System.nanoTime();
		Connection conn = null;
		ResultSet rs = null;
		ResultSet rs2 = null;
		Statement statement = null;
		Statement statement2 = null;
		ArrayList<String> sArrUsers = new ArrayList<String>();
		
		try {
		
		conn = datasource.getConnection();
		
			String queryString = userAttrQuery + "'" + sUserId + "'";	
			LOG.info(queryString);
			   
			statement = conn.createStatement();
			         
			rs = statement.executeQuery(queryString);
			
			// Returned: EMPLID, PRF_USERID, CNTRY_OF_CIT_NM , CNTRY_OF_CIT_NM2, EXT_CO_NM, SITE_NM, RLTNP_CD 
			
			if (rs.next()) {
			          
				LOG.info("Query successful for:"+rs.getString("EMPLID"));
			  // if (rs.getString("EMPLID").equals(sUserId)){
				    
				if (rs.getString("EMPLID") != ""){

				    String employerName = "";
	       	   
				   // *********** Magic: Spirit or a bad-entry for contractor *************
		           if ((rs.getString("RLTNP_CD")!=null) && (rs.getString("RLTNP_CD").equals(typeIsEmployee)) && rs.getString("SITE_NM")!=null){
		        		   employerName = "SPIRIT-" + rs.getString("SITE_NM");
		           } else if (((rs.getString("EXT_CO_NM") == null) || (rs.getString("EXT_CO_NM") == "")) ){
		        		   //********** Bad Record ********
		        		   employerName = "Unknown";
		           }  else {
	                  // ************ Non-Spirit vendor employer 
				      employerName = rs.getString("EXT_CO_NM");
		           }
		           
		           LOG.info("employerName" + employerName  );
		           
	        		if (employerName!=null){
	        			sArrUsers.add(0, employerName);
	        		}
	        		else{
	        			sArrUsers.add(0,"");
	        		}
				   
	        	   // employerName == "Unknown" will always return an empty result set.
				   queryString = "SELECT Vendor.CUST_CD FROM VENDOR_LOOK_UP Vendor"+ " WHERE Vendor.EMPLOYER_NM= '" +  employerName + "'";	
				   LOG.info(queryString);
				   
				   statement2 = conn.createStatement();	
				   rs2 = statement2.executeQuery(queryString);
				   
	        		if (rs2.next()) {
		        		if (rs2.getString("CUST_CD")!=null){
		        			sArrUsers.add(1,rs2.getString("CUST_CD"));
		        		} else{
		        			sArrUsers.add(1,"");
		        		}	
	        		} else {
	        			sArrUsers.add(1,"");
	        		}
	
			       // Now fill out rest of sArrUsers	
	        		if (rs.getString("CNTRY_OF_CIT_NM")!=null){
	        			sArrUsers.add(2,rs.getString("CNTRY_OF_CIT_NM"));
	        		}
	        		else{
	        			sArrUsers.add(2,"");
	        		}

	
	        		if (rs.getString("CNTRY_OF_CIT_NM2")!=null){
	        			sArrUsers.add(3,rs.getString("CNTRY_OF_CIT_NM2"));
	        		}
	        		else{
	        			sArrUsers.add(3,"");
	        		}
	        		
	        		if (rs.getString("SITE_NM")!=null){
	        			sArrUsers.add(4,rs.getString("SITE_NM"));
	        		}
	        		else{
	        			sArrUsers.add(4,"");
	        		}
	        		
	        	}
	        	else{
	        		LOG.warn("User Id did not match query response from CED. input:" + sUserId + ", query:"+ rs.getString("USERID"));
	        		return null;
	        	}
	        		
	        	LOG.debug("Have user record!!! for:" + sUserId);
	           return sArrUsers;        
	        }
	        else{
	        	LOG.debug("NO User RECORD !!! for:" + sUserId);
	        }
		} catch (Exception e) {
			LOG.error(e.toString(), e);
		}
		finally{
			try {
				if (null!=rs)
					rs.close();
				if (null!=rs2)
					rs2.close();
				if (null!=statement)
					statement.close();
				if (null!=statement2)
					statement2.close();
				if (conn!=null)
					conn.close();

			} catch (SQLException e) {}
			
			LOG.info("Time spent for query getUserAttributes = "+ ((System.nanoTime() - lCurrentTime)/1000000.00) +"ms");
		}
		
		return null;
	}
	
	
	/**
	 * Getting the license code from table NXL_EXPORT_LICENSE based on input criteria
	 * @param sWorkLocation
	 * @param sCitizhenship
	 * @param sCustomerCode
	 * @param sEccns
	 * @param sOems
	 * @return
	 */
	private String getEARLicenseCode(String sCitizhenship, String sCitizhenship2, String sCustomerCode, String sEccn){

		long lCurrentTime = System.nanoTime();
		Connection conn = null;
		Statement statement = null;
		ResultSet rs = null;
		String sOut = "";

		try {

			conn = datasource.getConnection();
			statement = conn.createStatement();

			StringBuffer sBufQuery = new StringBuffer();

			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date date = new Date();
			String sDate = "to_date('" + dateFormat.format(date) + "','yyyy-MM-dd')";

			sBufQuery.append("SELECT LIC_CD FROM LICENSE ")
			.append("WHERE ECCN_NO IN('").append(sEccn)
			.append("') AND CUST_CD='").append(sCustomerCode).append("' ");
			
			
			if (!sCitizhenship.equals("") && !sCitizhenship2.equals("")){
			
				sBufQuery.append(" AND CIT_CNTRY_NM IN('").append(sCitizhenship)
						.append("','").append(sCitizhenship2).append("') ");
			}
			else if (!sCitizhenship.equals("")){
				sBufQuery.append(" AND CIT_CNTRY_NM IN('").append(sCitizhenship)
				.append("') ");
			}
			else if (!sCitizhenship2.equals("")){
				sBufQuery.append(" AND CIT_CNTRY_NM IN('")
				.append(sCitizhenship2).append("') ");
			}
			
			
			sBufQuery.append(" AND VAL_DT <=").append(sDate)
			.append(" AND XPIR_DT >=").append(sDate);

			LOG.info(sBufQuery.toString());

			rs = statement.executeQuery(sBufQuery.toString());

			if (rs.next()) {
				sOut = rs.getString("LIC_CD");
				LOG.debug("License "+ sOut + " found!");
				return sOut;        
			}
			else{
				LOG.debug("NO License RECORD!!!");
			}
			

		} catch (Exception e) {
			LOG.error(e.toString(), e);
		}
		finally{
			try {
				if (null!=rs)
					rs.close();
				if (null!=statement)
					statement.close();
				if (conn!=null)
					conn.close();

			} catch (SQLException e) {}

			LOG.info("Time spent for query getEARLicenseCode = "+ ((System.nanoTime() - lCurrentTime)/1000000.00) +"ms");
		}

		return sOut;
	}
	
	
	/**
	 * Getting the license code from table NXL_EXPORT_LICENSE based on input criteria
	 * @param sWorkLocation
	 * @param sCitizhenship
	 * @param sCustomerCode
	 * @param sEccns
	 * @param sOems
	 * @return
	 */
	private String getLicenseRequired(String sEccns, String sAuthgrpcode){

		long lCurrentTime = System.nanoTime();
		Connection conn = null;
		Statement statement = null;
		ResultSet rs = null;

		try {
			conn = datasource.getConnection();
			statement = conn.createStatement();
			StringBuffer sBufQuery1 = new StringBuffer();
			sBufQuery1.append("SELECT LIC_FLG, AUTH_GRP_CD FROM ECCN WHERE ECCN_CD IN ('").
				append(sEccns).append("') AND AUTH_GRP_CD ='").
						append (sAuthgrpcode).
							append ("'");
		
			LOG.info(sBufQuery1.toString());
			
			rs = statement.executeQuery(sBufQuery1.toString());

			if (rs.next()){				
				
				if (rs.getString("LIC_FLG")!=null && 
						(rs.getString("LIC_FLG").equalsIgnoreCase("N"))){
					LOG.debug("No need to check for licensing");
					//Return 1 to indicate no license required;
					return "-2";
				}
				else{
					//Return 2 to indicate need to continue checking
					LOG.debug("Need to check for license");
					return rs.getString("AUTH_GRP_CD");
				}
			}
			else{
				//Will return 0 can causing DENY
				LOG.debug("ECCN input is not in the list");
			}


		} catch (Exception e) {
			LOG.error(e.toString());
		}
		finally{
			try {
				if (null!=rs)
					rs.close();
				if (null!=statement)
					statement.close();
				if (conn!=null)
					conn.close();

			} catch (SQLException e) {}

			LOG.info("Time spent for query getLicenseRequired = "+ ((System.nanoTime() - lCurrentTime)/1000000.00) +"ms");
		}

		return "-1";
	}


	/**
	 * Getting the OEM list from table NXL_LOCATION_OEM and compare with documents OEM. If
	 * any of the OEM value then return true.
	 * @param sWorkLocation
	 * @param sDocOems
	 * @return
	 */
	private boolean getOEMListByLocation(String sWorkLocation, String sAuthCode){

		long lCurrentTime = System.nanoTime();
		Connection conn = null;
		ResultSet rs = null;
		Statement statement = null;

		try {
			
			String sOemCode = sAuthCode.substring(0,1);
			
			//If not able to retrieve oem code then return false;
			if (sOemCode==null){
				LOG.error("Cannot retrieve OEMCode from properties file, please check. ");
				return false;
			}
				
			conn = datasource.getConnection();

			StringBuffer sBufQuery = new StringBuffer();

			sBufQuery.append("SELECT OEM_CD FROM OEM_LOCATION ")
			.append("WHERE LOC_NM ='").append(sWorkLocation)
			.append("' AND OEM_CD IN ('").append(sOemCode).append("')");

			LOG.info(sBufQuery.toString());

			statement = conn.createStatement();

			rs = statement.executeQuery(sBufQuery.toString());

			if (rs.next()) {
				return true;  
			}
			else{
				LOG.debug("NO OEM RECORD!!!");
				return false;
			}
		} catch (Exception e) {
			LOG.error(e.toString(), e);
		}
		finally{
			try {
				if (null!=rs)
					rs.close();
				if (null!=statement)
					statement.close();
				if (conn!=null)
					conn.close();

			} catch (SQLException e) {}

			LOG.info("Time spent for query getOEMListByLocation = "+ ((System.nanoTime() - lCurrentTime)/1000000.00) +"ms");
		}

		return false;
	}
		
	/**
	 * Setting up the connection pool for oracle database.
	 */
	public DataSource setupConnectionPool(String inUrl, String inUser, String inPassword){
		
		 DataSource datasource;
		 
		 PoolProperties p = new PoolProperties();
         p.setUrl(inUrl);
         p.setDriverClassName("oracle.jdbc.driver.OracleDriver");
         p.setUsername(inUser);
         p.setPassword(inPassword);
         p.setJmxEnabled(true);
         p.setTestWhileIdle(false);
         p.setTestOnBorrow(true);
         p.setValidationQuery("SELECT 1 FROM DUAL");
         p.setTestOnReturn(false);
         p.setValidationInterval(30000);
         p.setTimeBetweenEvictionRunsMillis(30000);
         p.setMaxActive(100);
         p.setInitialSize(5);
         p.setMaxWait(10000);
         p.setRemoveAbandonedTimeout(60);
         p.setMinEvictableIdleTimeMillis(30000);
         p.setMinIdle(10);
         p.setLogAbandoned(true);
         p.setRemoveAbandoned(true);
         p.setJdbcInterceptors(
           "org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"+
           "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");
         datasource = new DataSource();
         datasource.setPoolProperties(p);
         
         return datasource;
         
	}
	

	/**
	 * For testing purpose
	 * @param args
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) {

		LicenseValidationService plugin = new LicenseValidationService();
		
		String sDBUrl ="jdbc:oracle:thin:@//192.168.198.128:1521/orcl";
		String sDBUser="nextlabs";
		String sDBPassword="123next";
		
		plugin.setupConnectionPool(sDBUrl,sDBUser,sDBPassword);
//		ArrayList<String> strArr = new ArrayList<String>();
//		strArr.add("1E201");
//		strArr.add("B");
//		
//		IMultivalue iMultiEccn = Multivalue.create(strArr);
		ArrayList<String> strArrOem = new ArrayList<String>();
		strArrOem.add("Dell");
		strArrOem.add("Sony");
		
		IMultivalue iMultiOems = Multivalue.create(strArrOem);
		while(true){
		
		long lCurrentTime = System.nanoTime();
		
		IEvalValue[] sDataArr = new IEvalValue[3];
		sDataArr[0] = EvalValue.build("kentlee");
		sDataArr[1] = EvalValue.build("1E201");
		sDataArr[2] = EvalValue.build(iMultiOems);

		System.out.println(plugin.ValidateUserAccessToEARData(sDataArr));
		
		LOG.info("Time spent for AC = "+ ((System.nanoTime() - lCurrentTime)/1000000.00) +"ms");
		
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {}

		}
	}
}