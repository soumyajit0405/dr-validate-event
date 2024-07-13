
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

class EventCustomerThread implements Runnable {
	public  int customerId;
	public int eventCustomerMapping;
	public  String startTime;
	public int eventId;
	public String endTime;
	private static String url = "http://139.59.30.90:3020/data_warehouse/meter/fetchDatasnapshot";
	final long ONE_MINUTE_IN_MILLIS = 60000;// millisecs
	final long HOUR = 3600 * 1000; // in milli-seconds
	final long HALFHOUR = 1800 * 1000;
	
	public EventCustomerThread(int customerId,int eventCustomerMapping, String startTime, int eventId, String endTime) {
		this.customerId = customerId;
		this.eventCustomerMapping = eventCustomerMapping;
		this.startTime = startTime;
		this.eventId = eventId;
		this.endTime = endTime;
	}

	public void run() {
		try {
		ScheduleDAO sdc= new ScheduleDAO();
//			ArrayList<HashMap<String,Object>> listOfCustomerData=sdc.getEventCustomerData(customerId, eventId);
//			//Invoke node Api
//			System.out.println("Start EventCustomerThread");
//			if (listOfCustomerData.size() > 0) {
//
//				ExecutorService executor = Executors.newFixedThreadPool(listOfCustomerData.size());// creating a pool of 1000
//																							// threads
//				for (int i = 0; i < listOfCustomerData.size(); i++) {
//					Runnable worker = new EventCustomerSwitchesThread(
//							(String)listOfCustomerData.get(i).get("kiotDeviceId"),
//							(int)listOfCustomerData.get(i).get("kiotUserMappingId"),
//							(String)listOfCustomerData.get(i).get("kiotUserId"),
//							(String)listOfCustomerData.get(i).get("bearerToken"),
//							(String)listOfCustomerData.get(i).get("customData"),
//							"",
//							eventId, customerId);
//					System.out.println("List of run EventCustomerThread");
//					executor.execute(worker);// calling execute method of ExecutorService
//				}
//				executor.shutdown();
//				while (!executor.isTerminated()) {
//				}
//
//		}
//			TimeUnit.SECONDS.sleep(10);
		ScheduleDAO sd =new ScheduleDAO();
		if (sd.getContractType(customerId) == 2) {
		System.out.println("Before Start Trx");
			getStartTxData();
			System.out.println("Before End Trx");
			getTxData();
		} else {
			System.out.println("Before Start Trx KIOT");
			getStartTxDataFromKiot();
			System.out.println("Before End Trx KIOT");
		//	getTxDataFromKiot();
		}
		 
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			if (ScheduleDAO.con != null) {
//				try {
//			//		ScheduleDAO.con.close();  Close later
//				} catch (SQLException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			}
				}
		System.out.println(Thread.currentThread().getName() + " (End)");// prints thread name
	}

	private void processmessage() {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void getTxData() throws ClassNotFoundException, SQLException, JSONException, IOException, ParseException {
		ScheduleDAO scd= new ScheduleDAO();
		JSONObject response = null;
		if (scd.getEventCustomerStatus(customerId, eventId) != 12) {

			JSONArray meterArray = new JSONArray();
			double meterReading=0;
			DBHelper dbhelper = new DBHelper();
			HttpConnectorHelper httpconnectorhelper= new HttpConnectorHelper();
//			JSONObject inputDetails1= new JSONObject();
//			inputDetails1.put("meterId", 6);
//			//inputDetails1.put("timestamp", "2020-06-24 14:30:00");
//			//inputDetails1.put("timestamp", this.startTime);
//			inputDetails1.put("timestamp", this.endTime);
//			System.out.println("Time in End Tx Date"+this.endTime);
//			ArrayList<JSONObject> responseFromDevice = httpconnectorhelper
//					.sendPostWithToken(scd.getConnectionString(customerId,eventId), inputDetails1);
//			// HashMap<String,String> responseAfterParse =
//			// cm.parseInput(responseFrombcnetwork);
//			System.out.println("Response for Agent" +responseFromDevice);
//			if (responseFromDevice.size() == 1) {
//				dbhelper.updateEndTradeEventCustomer(meterReading,eventId,customerId,"e", this.endTime);
//			} else {
//				JSONObject js1 = (JSONObject) responseFromDevice.get(0);
//				System.out.println("js1 --- "+ js1);
//			if(js1.isNull("meterData")) {
//				meterReading = 0;
//			} else {
//				
//				meterArray = (JSONArray)js1.get("meterData");
//				System.out.println("meterArray --- "+ meterArray);
//				if (meterArray.length() > 0) {
//				JSONObject js =(JSONObject) meterArray.get(0);
//				try {
//					meterReading = (double)js.get("meterReading");
//					} catch (ClassCastException e ) {
//					meterReading = (int)js.get("meterReading");
//					}
//				}
//				
//			}
//			System.out.println("meter Reading --- "+ meterReading);
//				dbhelper.updateEndTradeEventCustomer(meterReading,eventId,customerId,"ne", this.startTime);
//			}
			int meterId = scd.getMeterId(customerId, eventId);
			ArrayList<String> dateRanges = getStartAndEndDates();
			JSONObject requestObject = new JSONObject();
			requestObject.put("meterId", meterId);
			requestObject.put("timeslot", dateRanges.get(2));
			requestObject.put("queryDate", dateRanges.get(0));
			response = httpconnectorhelper.sendRequestToAgent(url, requestObject, 1);
			if (response == null || response.has("msg") || response.has("errormessage")) {
				dbhelper.updateEventCustomer(meterReading,eventId,customerId,"e");
			} else {
				JSONObject data = (JSONObject) response.get("dataSanpshot");
				if (!data.get("Active_I_Total_kWh").equals(null)) {
					Object meterReadingo = (Object) data.get("Active_I_Total_kWh");
					if(meterReadingo instanceof Integer) {
						meterReading = (int)meterReadingo;
					} if (meterReadingo instanceof Double) {
						meterReading = (double)meterReadingo;
					}
					//meterReading = (int) data.get("Active_I_Total_kWh");
					
				} else {
					meterReading = 0;
				}
				System.out.println("End Reading " +meterReading);
				//dbhelper.updateEventCustomer(meterReading,eventId,customerId,"ne");
				dbhelper.updateEndTradeEventCustomer(meterReading,eventId,customerId,"ne", this.startTime, meterId);
			}
		}	
	}
	
	public void getStartTxData() throws ClassNotFoundException, SQLException, JSONException, IOException, ParseException {
		ScheduleDAO scd= new ScheduleDAO();
		JSONObject response = null;
		if (scd.getEventCustomerStatus(customerId, eventId) != 12) {

			JSONArray meterArray = new JSONArray();
			double meterReading=0;
			ArrayList<String> dateRanges = getStartAndEndDates();
			DBHelper dbhelper = new DBHelper();
			HttpConnectorHelper httpconnectorhelper= new HttpConnectorHelper();
//			JSONObject inputDetails1= new JSONObject();
//			inputDetails1.put("meterId", 6);
//			//inputDetails1.put("timestamp", "2020-06-24 14:15:00");
//		//	inputDetails1.put("timestamp", this.endTime);
//			inputDetails1.put("timestamp", this.startTime);
//			System.out.println("Time in Start Tx Date"+this.startTime);
//			ArrayList<JSONObject> responseFromDevice = httpconnectorhelper
//					.sendPostWithToken(scd.getConnectionString(customerId, eventId), inputDetails1);
			
			JSONObject requestObject = new JSONObject();
			requestObject.put("meterId", scd.getMeterId(customerId, eventId));
			requestObject.put("timeslot", dateRanges.get(1));
			requestObject.put("queryDate", dateRanges.get(0));
			response = httpconnectorhelper.sendRequestToAgent(url, requestObject, 1);
			if (response == null || response.has("msg") || response.has("errormessage")) {
				dbhelper.updateEventCustomer(meterReading,eventId,customerId,"e");
			} else {
				JSONObject data = (JSONObject) response.get("dataSanpshot");
				if (!data.get("Active_I_Total_kWh").equals(null)) {
					Object meterReadingo = (Object) data.get("Active_I_Total_kWh");
					if(meterReadingo instanceof Integer) {
						meterReading = (int)meterReadingo;
					} if (meterReadingo instanceof Double) {
						meterReading = (double)meterReadingo;
					}
					//meterReading = (int) data.get("Active_I_Total_kWh");
					System.out.println("Start Reading " +meterReading);	
				} else {
					meterReading = 0;
				}
				dbhelper.updateEventCustomer(meterReading,eventId,customerId,"ne");
			}
			// HashMap<String,String> responseAfterParse =
			// cm.parseInput(responseFrombcnetwork);
			
//			if (responseFromDevice.size() == 1) {
//				dbhelper.updateEventCustomer(meterReading,eventId,customerId,"e");
//			} else {
//				JSONObject js1 = (JSONObject) responseFromDevice.get(0);
//			if(js1.isNull("meterData")) {
//				meterReading = 0;
//			} else {
//				meterArray = (JSONArray)js1.get("meterData");
//				if (meterArray.length() > 0) {
//				JSONObject js =(JSONObject) meterArray.get(0);
//				try {
//					meterReading = (double)js.get("meterReading");
//					} catch (ClassCastException e ) {
//					meterReading = (int)js.get("meterReading");
//					}
//				}
//				
//			}
//				dbhelper.updateEventCustomer(meterReading,eventId,customerId,"ne");
//			
//		}	
		}
	}
	

	private ArrayList<String> getStartAndEndDates() throws ParseException {
		
        
		
		ArrayList<String> dateRanges = new ArrayList<>();
		SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		// SimpleDateFormat dt1 = new SimpleDateFormat("2018-03-06");
		SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm");
		Date startDate = dateTimeFormat.parse(this.startTime);
		String sR1 = timeFormat.format(startDate);
		Date startDateR1 = new Date(startDate.getTime() - 15 * ONE_MINUTE_IN_MILLIS);
		String sR2 = timeFormat.format(startDateR1);
		String startDateRange = sR2 + "-" + sR1;

		Date endDate = dateTimeFormat.parse(this.endTime);
		String eR1 = timeFormat.format(endDate);
		Date endDateR1 = new Date(endDate.getTime() - 15 * ONE_MINUTE_IN_MILLIS);
		String eR2 = timeFormat.format(endDateR1);
		String endDateRange = eR2 + "-" + eR1;
		dateRanges.add(dateFormat.format(startDate));
		dateRanges.add(startDateRange);
		dateRanges.add(endDateRange);
		return dateRanges;

	}
	
	public void getStartTxDataFromKiot() throws ClassNotFoundException, SQLException, JSONException, IOException {
		ScheduleDAO scd= new ScheduleDAO();
		if (scd.getEventCustomerStatus(customerId, eventId) != 12) {
			ArrayList<String> deviceIds = scd.getDeviceIds(customerId,eventCustomerMapping);
			String[] values = startTime.split("-");
			String[] dateTime =values[2].split(" "); 
			String[] time = dateTime[1].split(":");
			double meterReading=0;
			DBHelper dbhelper = new DBHelper();
			HttpConnectorHelper httpconnectorhelper= new HttpConnectorHelper();
			JSONObject input= new JSONObject();
			JSONObject payload= new JSONObject();
			JSONObject filterSlots = new JSONObject();
			filterSlots.put("hour", Integer.parseInt(time[0]));
			filterSlots.put("minute", Integer.parseInt(time[1]));	
			payload.put("duration", "day");
			payload.put("group_by_minutes", "15");
			payload.put("year", Integer.parseInt(values[0]));
			payload.put("month", Integer.parseInt(values[1]));
			payload.put("startDay", Integer.parseInt(dateTime[0]));
			payload.put("filter_slot", filterSlots);
			payload.put("device_ids", deviceIds);
			input.put("payload", payload);
			input.put("intent", "action.entities.ENERGY_DATA");
			ArrayList<JSONObject> responseFromDevice = httpconnectorhelper
					.sendPostWithTokenForKiot("https://api.kiot.io/integrations/ctp/go",input, scd.getBearerToken(customerId));
			// HashMap<String,String> responseAfterParse =
			if (responseFromDevice.size() == 1 &&  (boolean)responseFromDevice.get(0).get("error") == true) {
				dbhelper.updateKiotEventCustomer(meterReading,eventId,customerId,"e");
			} else {
			JSONArray jsArr = (JSONArray) responseFromDevice.get(0).get("data");
			 System.out.println(responseFromDevice.get(0).get("data"));
			 if (jsArr.length() > 0) {
			 int energy = getEnergy((JSONArray)responseFromDevice.get(0).get("data"), time[0], time[1]);
			 meterReading = (double)energy;
			 } else {
				 meterReading = 0;	 
			 }
			 
			if ((boolean)responseFromDevice.get(1).get("error")) {
				dbhelper.updateKiotEventCustomer(meterReading,eventId,customerId,"e");
			} else {
				//dbhelper.updateEventCustomer(meterReading,eventId,customerId,"ne");
				dbhelper.updateKiotEnergy(meterReading,eventId,customerId,"ne");
			}
			}
		}
			
		}	
		
	
	public int getEnergy(JSONArray data, String hour, String minute) throws JSONException {
		for (int i=0;i<data.length();i++) {
			JSONObject js =(JSONObject)data.get(i);
			JSONObject id= (JSONObject)js.get("_id");	
			if ((int)id.get("hour") ==  Integer.parseInt(hour) && (int)id.get("minute") ==  Integer.parseInt(minute)) {
				return (int)js.get("avg_energy");
			}
		}
		return 0;
	}

	public static void main(String args[]) throws ClassNotFoundException, SQLException, JSONException, IOException {
	//	EventCustomerThread ect= new EventCustomerThread(1, "2020-07-04 18:00:00", 1, "2020-07-04 18:00:00");
	//	ect.getStartTxDataFromKiot("w-06-15 18:00:00");
		String sample="[{\"msg\":\"success\",\"meterData\":[{\"date\":\"2020-07-26T18:30:00.000Z\",\"meterId\":6,\"meterReading\":12.2,\"txnInfo3\":null,\"txnInfo2\":0.126,\"txnInfo5\":null,\"txnInfo4\":null,\"timestamp\":\"18:45-19:00\"}]}, {\"error\":false}]";
		JSONArray js2 = new JSONArray(sample);
		JSONObject js1 = (JSONObject) js2.get(0);
		double meterReading = 0;
		JSONArray meterArray = new JSONArray();
		if (js2.length() == 1) {
			
		} else {
			if(js1.isNull("meterData")) {
				meterReading = 0;
			} else {
				JSONObject js4=(JSONObject)js2.get(0);
				meterArray = (JSONArray)js4.get("meterData");
				if (meterArray.length() > 0) {
				JSONObject js =(JSONObject) meterArray.get(0);
				try {
					meterReading = (double)js.get("meterReading");
					} catch (ClassCastException e ) {
					meterReading = (int)js.get("meterReading");
					}
				}
				
			}
			
		}
		System.out.println((boolean)js1.get("error"));
	}

}