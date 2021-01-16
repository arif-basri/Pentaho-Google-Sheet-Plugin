/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.pentaho.di.trans.steps.pentahogooglesheets;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.sheets.v4.model.*;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.core.Const;


import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpHeaders;


import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.Permission;


import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Describe your step plugin.
 * 
 */

public class PentahoGoogleSheetsPluginOutput extends BaseStep implements StepInterface {

  private static Class<?> PKG = PentahoGoogleSheetsPluginInput.class; // for i18n purposes, needed by Translator2!!
  
  private PentahoGoogleSheetsPluginOutputMeta meta;
  private PentahoGoogleSheetsPluginOutputData data;
  
  public PentahoGoogleSheetsPluginOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }
  
  /**
     * Initialize and do work where other steps need to wait for...
     *
     * @param stepMetaInterface
     *          The metadata to work with
     * @param stepDataInterface
     *          The data to initialize
     */
   @Override
   public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
        
		meta = (PentahoGoogleSheetsPluginOutputMeta) smi;
        data = (PentahoGoogleSheetsPluginOutputData) sdi;
        JsonFactory JSON_FACTORY;
		NetHttpTransport HTTP_TRANSPORT;
		String APPLICATION_NAME = "pentaho-sheets";
		String TOKENS_DIRECTORY_PATH = Const.getKettleDirectory() +"/tokens";
        String scope;
		Boolean exists=false;
	    List<Request> requests = new ArrayList<>();
     
	    try {
   	   	    JSON_FACTORY = JacksonFactory.getDefaultInstance();
			HTTP_TRANSPORT=GoogleNetHttpTransport.newTrustedTransport();			
		} catch (Exception e) {
			logError("Exception",e.getMessage(),e);
		}

		if (super.init(smi, sdi)) {
				
				//Check if file exists
				logDebug("Checking if Spreadsheet exists.");
				 try {
                    HTTP_TRANSPORT=GoogleNetHttpTransport.newTrustedTransport();
				    APPLICATION_NAME = "pentaho-sheets";
                    JSON_FACTORY = JacksonFactory.getDefaultInstance();
                    TOKENS_DIRECTORY_PATH = Const.getKettleDirectory() +"/tokens";
					 //Init Drive Service
                    scope="https://www.googleapis.com/auth/drive";
					Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, PentahoGoogleSheetsPluginCredentials.getCredentialsJson(scope,environmentSubstitute(meta.getJsonCredentialPath()))).setApplicationName(APPLICATION_NAME).build();
					logDebug("Drive API service initiated");
					 //Init Sheet Service
				 	scope="https://www.googleapis.com/auth/spreadsheets";
					data.service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, PentahoGoogleSheetsPluginCredentials.getCredentialsJson(scope,environmentSubstitute(meta.getJsonCredentialPath()))).setApplicationName(APPLICATION_NAME).build();
					logDebug("Sheets API service initiated");

					String wsID=environmentSubstitute(meta.getSpreadsheetKey());
                    logDebug("Spreadsheet ID: "+wsID);
					//"properties has { key='id' and value='"+wsID+"'}";
					String q="mimeType='application/vnd.google-apps.spreadsheet'";
					FileList result = service.files().list().setSupportsAllDrives(true).setIncludeItemsFromAllDrives(true).setQ(q).setPageSize(100).setFields("nextPageToken, files(id, name)").execute();
                    List<File> spreadsheets = result.getFiles();

					 for (File spreadsheet:spreadsheets) {
//                        logDebug(wsID+" VS "+spreadsheet.getId());
						if(wsID.equals(spreadsheet.getId()))
						{
							exists=true; //file exists
							logBasic("Spreadsheet:"+ wsID +" exists");
						}

                    }
					//If it does not exist & create checkbox is checker create it.
					//logBasic("Create if Not exist is :"+meta.getCreate());
					if(!exists && meta.getCreate())
					{
						if(!meta.getAppend()){ //si append + create alors erreur

						//If it does not exist create it.
						Spreadsheet spreadsheet = new Spreadsheet().setProperties(new SpreadsheetProperties().setTitle(wsID));
						Spreadsheet response = data.service.spreadsheets().create(spreadsheet).execute();
						String spreadsheetID = response.getSpreadsheetId();
						meta.setSpreadsheetKey(spreadsheetID);
						logDebug("Spreadsheet ID:"+spreadsheetID);
						//logBasic(response);
						//If it does not exist we use the Worksheet ID to rename 'Sheet ID'
						if(environmentSubstitute(meta.getWorksheetId())!="Sheet1")
						{
							SheetProperties title = new SheetProperties().setSheetId(0).setTitle(environmentSubstitute(meta.getWorksheetId()));
							// make a request with this properties
							UpdateSheetPropertiesRequest rename = new UpdateSheetPropertiesRequest().setProperties(title);
							// set fields you want to update
							rename.setFields("title");
							logBasic("Changing worksheet title to:"+ environmentSubstitute(meta.getWorksheetId()));

							requests.clear();
							Request request1 = new Request().setUpdateSheetProperties(rename);
							requests.add(request1);
							BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
							requestBody.setRequests(requests);
		                     // now you can execute batchUpdate with your sheetsService and SHEET_ID
							data.service.spreadsheets().batchUpdate(spreadsheetID, requestBody).execute();
							//now if share email is not null we share with R/W with the email given
							if((environmentSubstitute(meta.getShareEmail())!=null && !environmentSubstitute(meta.getShareEmail()).isEmpty()) || (environmentSubstitute(meta.getShareDomain())!=null && !environmentSubstitute(meta.getShareDomain()).isEmpty()))
							{

								String fileId=spreadsheetID;
								JsonBatchCallback<Permission> callback = new JsonBatchCallback<Permission>() {
								  @Override
								  public void onFailure(GoogleJsonError e,
														HttpHeaders responseHeaders)
									  throws IOException {
									// Handle error
									logError("Failed sharing file" + e.getMessage());
								  }

								  @Override
								  public void onSuccess(Permission permission,
														HttpHeaders responseHeaders)
									  throws IOException {
									logBasic("Shared successfully : Permission ID: " + permission.getId());
								  }
								};
								BatchRequest batch = service.batch();
								if(environmentSubstitute(meta.getShareEmail())!=null && !environmentSubstitute(meta.getShareEmail()).isEmpty())
								{
								logBasic("Sharing sheet with:"+ environmentSubstitute(meta.getShareEmail()));
								Permission userPermission = new Permission()
									.setType("user")
									.setRole("writer")
									.setEmailAddress(environmentSubstitute(meta.getShareEmail()));
								//Using Google drive service here not spreadsheet data.service
								service.permissions().create(fileId, userPermission)
									.setFields("id")
									.queue(batch, callback);
								}
								if(environmentSubstitute(meta.getShareDomain())!=null && !environmentSubstitute(meta.getShareDomain()).isEmpty())
								{
									logBasic("Sharing sheet with domain:"+environmentSubstitute(meta.getShareDomain()));
									Permission domainPermission = new Permission()
									.setType("domain")
									.setRole("reader")
									.setDomain(environmentSubstitute(meta.getShareDomain()));
								service.permissions().create(fileId, domainPermission)
									.setFields("id")
									.queue(batch, callback);

								}
								batch.execute();

							}

						}
					  } else {
					    	logError("Append and Create options cannot be activated alltogether");
					    	return false;
					  }

					}

					if(!exists && !meta.getCreate())
					{
						logError("File does not Exist");
						return false;
					}


					 //get source and hidden sheet properties
					 logDebug("Getting the Worksheet ID of current worksheet and hidden worksheet");
					 Spreadsheet response1= data.service.spreadsheets().get(wsID).setIncludeGridData(false).execute();
					 List<Sheet> worksheets = response1.getSheets();
					 String[] names = new String[worksheets.size()];
					 int sourceSheetID = -1;
					 int selectedSheetID = -1;
					 for (int j = 0; j < worksheets.size(); j++) {
						 Sheet sheet = worksheets.get(j);
						 names[j] = sheet.getProperties().getTitle();
						 if (sheet.getProperties().getTitle().equals("HiddenForFormulaPurposes")) {
							 selectedSheetID = sheet.getProperties().getSheetId();
							 logDebug("Found hidden worksheet: "+names[j]+" (ID:"+selectedSheetID+")");
						 }
						 if (environmentSubstitute(meta.getWorksheetId()).equals(sheet.getProperties().getTitle())){
						 	sourceSheetID = sheet.getProperties().getSheetId();
							 logDebug("Found current worksheet: "+names[j]+" (ID:"+sourceSheetID+")");
						 }
					 }

					 //create hidden sheet if it not exists
					 if (selectedSheetID == -1) {
						 logDebug("\nHidden worksheet not found! \nCreating hidden Worksheet");
						 requests.add(new Request().setAddSheet(
								 new AddSheetRequest()
										 .setProperties(new SheetProperties()
												 .setHidden(Boolean.TRUE)
												 .setTitle("HiddenForFormulaPurposes"))));
						 BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
						 requestBody.setRequests(requests);
						 BatchUpdateSpreadsheetResponse requestResponse = data.service.spreadsheets()
								 .batchUpdate(wsID, requestBody).execute();
						 selectedSheetID = requestResponse.getReplies().get(0).getAddSheet().getProperties()
								 .getSheetId();
						 logDebug("Hidden worksheet created: HiddenForFormulaPurposes (ID:"+selectedSheetID+")" );
					 }
					 data.hiddenSheetId = selectedSheetID;
					 data.sourceSheetId = sourceSheetID;

            } catch (Exception e) {
                logError("Error: for worksheet : "+environmentSubstitute( meta.getWorksheetId())+" in spreadsheet :"+environmentSubstitute( meta.getSpreadsheetKey()) + e.getMessage(), e);
                setErrors(1L);
                stopAll();
                return false;
            }

            return true;
        }
        return false;
    }

	public String getExcelColumnName (int columnNumber)
	{
		logDebug("Getting label of the AppsId column");
		int dividend = columnNumber;
		int i;
		String columnName = "";
		int modulo;
		while (dividend > 0)
		{
			modulo = (dividend - 1) % 26;
			i = 65 + modulo;
			columnName = new Character((char)i).toString() + columnName;
			dividend = (int)((dividend - modulo) / 26);
		}
		logDebug("Label of the AppsId column is: "+columnName);
		return columnName;
	}
	public static boolean isInteger(String s) {
		boolean isValidInteger = false;
		try
		{
			Integer.parseInt(s);

			// s is a valid integer

			isValidInteger = true;
		}
		catch (NumberFormatException ex)
		{
			// s is not an integer
		}

		return isValidInteger;
	}
	public int findRowIndex(String sheetToSearch, String columnToSearch, String valueToFind) throws IOException, GeneralSecurityException {
   	    logRowlevel("Finding row index to be deleted");
		String majorDimensions = "ROWS";
		String valueInputOption = "USER_ENTERED";//"RAW";
		List<List<Object>> values = new ArrayList<>();

		String spreadsheetId=environmentSubstitute(meta.getSpreadsheetKey());
		String hiddenSheetRange = "HiddenForFormulaPurposes!A1";
		ArrayList<Object> listOfValues = new ArrayList<>();
		String formulaValue;
		ValueRange body;
		UpdateValuesResponse result;
		String rowIndex;
		NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
		JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
		String APPLICATION_NAME = "pentaho-sheets";
		String TOKENS_DIRECTORY_PATH = Const.getKettleDirectory() + "/tokens";
		String scope = SheetsScopes.SPREADSHEETS;
		data.service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, PentahoGoogleSheetsPluginCredentials.getCredentialsJson(scope,environmentSubstitute(meta.getJsonCredentialPath()))).setApplicationName(APPLICATION_NAME).build();

		formulaValue = "=MATCH(\""+valueToFind+"\", "+sheetToSearch+"!"+columnToSearch+", 0)"; //"=MATCH(\"ce6be55b-7588-47d8-be74-2d10aca6ac1c\", Sheet3!K:K, 0)";
		listOfValues.add(formulaValue);
		values.add(listOfValues);
		body = new ValueRange()
				.setValues(values)
				.setMajorDimension(majorDimensions);

		result = data.service.spreadsheets().values().update(spreadsheetId, hiddenSheetRange, body)
						.setValueInputOption(valueInputOption)
						.setIncludeValuesInResponse(Boolean.TRUE)
						.execute();
		rowIndex = result.getUpdatedData().getValues().get(0).get(0).toString();

		if (isInteger(rowIndex)) {
			logRowlevel("Row index to be deleted:"+rowIndex);
			return Integer.parseInt(rowIndex);
		}
		else
		{
			logRowlevel("__AppsId__ value not found.");
			return -1;
		}
	}

	public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    
	meta = (PentahoGoogleSheetsPluginOutputMeta) smi;
    data = (PentahoGoogleSheetsPluginOutputData) sdi;
	
	Object[] row = getRow();
    List<Object> r;

    logDebug("\n\tProcessing row:  "+data.currentRow);
    if (first && row!=null) {
		first = false;
		
		data.outputRowMeta=getInputRowMeta().clone();
		meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);	
		data.rows =  new ArrayList<List<Object>>();
		data.rowsId =  new ArrayList<Object>();
		if(meta.getAppend()){ //If append is checked we do not write the header
		   logBasic("Appending lines so skipping the header");
		   data.currentRow++;	
		} else {
			logBasic("Writing header");
			r= new ArrayList<Object>();

			for (int i = 0; i < data.outputRowMeta.size(); i++) {
				ValueMetaInterface v = data.outputRowMeta.getValueMeta(i);
				r.add(v.getName());
				if (v.getName().equals("__PowerAppsId__")){
					data.idColIdx = i;
					logBasic("__AppsId__ column index :"+data.idColIdx);
					data.appIDColumn = getExcelColumnName(data.idColIdx+1);
				}
			}
			data.rows.add(r);
			data.currentRow++;	
		}
		
	}
	try {
				//if last row is reached
				if (row == null) { //process the set of all rows
					logBasic("This is last row processing");
					if (meta.getDeleteRow()) {//skip this writing all phase if executing the step for deletion
						logBasic("Skipping last row processing due to deletion steps don't need it");
					}
					else if (meta.getBulkDeleteRow() && data.uUIDFirst!=null && data.uUIDLast!=null) {
						logBasic("This is BULK Delete");
						//delete row based on the key value
						logRowlevel("Deleting many rows in 1 request");
						List<Request> requests = new ArrayList<>();
						String spreadsheetId = meta.getSpreadsheetKey();

						String toDeleteColumnName = data.appIDColumn;
						toDeleteColumnName += ":"+toDeleteColumnName;
						int toDeleteRowIdxStart = findRowIndex(meta.getWorksheetId(),toDeleteColumnName,data.uUIDFirst);
						int toDeleteRowIdxEnd = findRowIndex(meta.getWorksheetId(),toDeleteColumnName,data.uUIDLast);
						logRowlevel("Rows to delete from :"+toDeleteRowIdxStart+ " to "+toDeleteRowIdxEnd);
						if (toDeleteRowIdxStart>-1 && toDeleteRowIdxEnd>-1) {

							//delete the row
							requests.clear();
							requests.add(new Request().setDeleteDimension(
									new DeleteDimensionRequest()
											.setRange(new DimensionRange()
													.setSheetId(data.sourceSheetId)
													.setDimension("ROWS")
													.setStartIndex(toDeleteRowIdxStart-1) //0-based
													.setEndIndex(toDeleteRowIdxEnd)
											)
									)
							);
							BatchUpdateSpreadsheetRequest body =
									new BatchUpdateSpreadsheetRequest().setRequests(requests);
							BatchUpdateSpreadsheetResponse response =
									data.service.spreadsheets().batchUpdate(spreadsheetId, body).execute();
							logRowlevel("Deletion response:"+response);

						}
					}
					else
						{
						if (data.currentRow > 0) {
							logBasic("At least there is one row that has been processed. ");
							ClearValuesRequest requestBody = new ClearValuesRequest();
							String range = environmentSubstitute(meta.getWorksheetId());

							//Creating Sheets service
							logBasic("Creating Sheets service");
							NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
							JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
							String APPLICATION_NAME = "pentaho-sheets";
							String TOKENS_DIRECTORY_PATH = Const.getKettleDirectory() + "/tokens";
							String scope = SheetsScopes.SPREADSHEETS;
							data.service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, PentahoGoogleSheetsPluginCredentials.getCredentialsJson(scope, environmentSubstitute(meta.getJsonCredentialPath()))).setApplicationName(APPLICATION_NAME).build();

							if (meta.getClearBeforeWrite()) //if Append is not checked we clear the sheet and we write content
							{//clear and write
								//Clearing existing Sheet
								Sheets.Spreadsheets.Values.Clear request = data.service.spreadsheets().values().clear(environmentSubstitute(meta.getSpreadsheetKey()), range, requestBody);
								logBasic("Clearing Sheet:" + range + "in Spreadsheet :" + environmentSubstitute(meta.getSpreadsheetKey()));
								if (request != null) {
									ClearValuesResponse response = request.execute();
								} else logBasic("Nothing to clear");

								//Writing Sheet
								logBasic("Writing to Sheet");
								ValueRange body = new ValueRange().setValues(data.rows); //remove the rest of the column and set the range to be only the id column
								String valueInputOption = "USER_ENTERED";
								UpdateValuesResponse result = data.service.spreadsheets().values().update(environmentSubstitute(meta.getSpreadsheetKey()), range, body).setValueInputOption(valueInputOption).execute();
							} else if (meta.getAppend()) {
								//Appending if option is checked

								// How the input data should be interpreted.
								String valueInputOption = "USER_ENTERED"; // TODO: Update placeholder value.

								// How the input data should be inserted.
								String insertDataOption = "INSERT_ROWS"; // TODO: Update placeholder value.

								// TODO: Assign values to desired fields of `requestBody`:
								ValueRange body = new ValueRange().setValues(data.rows);
								logBasic("Appending data :" + range + "in Spreadsheet :" + environmentSubstitute(meta.getSpreadsheetKey()));

								Sheets.Spreadsheets.Values.Append request = data.service.spreadsheets().values().append(environmentSubstitute(meta.getSpreadsheetKey()), range, body);
								request.setValueInputOption(valueInputOption);
								request.setInsertDataOption(insertDataOption);
								AppendValuesResponse response = request.execute();
								logBasic("Appending result:"+response);
							} else if (meta.getUpdateKeyField()) {//Update refID field only
								//Writing Sheet
								logBasic("Updating __AppsID__ field value");
								List<List<Object>> values = new ArrayList<List<Object>>();
								values.add(data.rowsId);
								//clone data.rows only the id column only
								ValueRange body = new ValueRange()
										.setValues(values)
										.setMajorDimension("COLUMNS");
								String rangeId = environmentSubstitute(meta.getWorksheetId()).concat("!").concat(data.appIDColumn).concat("2"); //"!J2";
								String valueInputOption = "RAW";
								UpdateValuesResponse result = data.service.spreadsheets().values().update(environmentSubstitute(meta.getSpreadsheetKey()), rangeId, body).setValueInputOption(valueInputOption).execute();
								logBasic("Updating result:"+result);

							} else {//update without clearing

								//Writing Sheet
								logBasic("Updating the Sheet without clearing");
								//clone data.rows on the id column only
								ValueRange body = new ValueRange().setValues(data.rows); //
								String valueInputOption = "RAW";
								UpdateValuesResponse result = data.service.spreadsheets().values().update(environmentSubstitute(meta.getSpreadsheetKey()), range, body).setValueInputOption(valueInputOption).execute();
								logBasic("Overwriting result:"+result);

							}
						} else {
							logBasic("No data found");
						}
					}
					logBasic("Marking this step as done");
					setOutputDone();

					return false;

				} else //process each row
					{
					r= new ArrayList<Object>();
					logRowlevel("Traversing the columns of current row");

					for (int i = 0; i < data.outputRowMeta.size(); i++) {
						int length=row.length;
						logRowlevel("Row length:"+length+" VS rowMeta "+data.outputRowMeta.size()+" i="+i);
						
						//check if the column is an id field and if it's blank, set the uuid value
						if (i==data.idColIdx) {
							Object valueCol = row[i];
							final String uuid = UUID.randomUUID().toString(); //.replace("-", "");

							if (valueCol == null||valueCol.toString().isEmpty()) {
								data.rowsId.add(uuid); //used for writing the id to the sheet
								r.add(uuid);
								row[i] = uuid; //update the generated id into the input row to be passed to the next step
								logRowlevel("New UUID value: "+uuid);

								//bulk deletion - set a flag to indicate a blank AppsID row has been found after a non-blank one.
								if (data.uUIDFirst!=null) {
									data.isStopBulkDelete = true;
									logDebug("Bulk deletion will execute only until previous row, due to blank AppsID value encountered in this row");
								}

							}else{
								String uuidCurrent = valueCol.toString();
								data.rowsId.add(uuidCurrent); //since can't use null value to skip cell update, copy existing value and rewrite the id to the sheet
								r.add(uuidCurrent);
								logRowlevel("Existing UUID value: "+uuidCurrent);

								//bulk deletion - only deals with continuous containing key id
								if (!data.isStopBulkDelete || data.uUIDFirst==null) {
									if (data.uUIDFirst == null) {
										logDebug("Setting the first UUID for bulk deletion");
										data.uUIDFirst = uuidCurrent;
									}
									data.uUIDLast  = uuidCurrent;
								}

							    if(meta.getDeleteRow())
								{//delete row based on the key value
									logRowlevel("Deleting current row");
									List<Request> requests = new ArrayList<>();
									String spreadsheetId = meta.getSpreadsheetKey();

									String toDeleteColumnName = data.appIDColumn;
									toDeleteColumnName += ":"+toDeleteColumnName;
									int toDeleteRowIdx = findRowIndex(meta.getWorksheetId(),toDeleteColumnName,uuidCurrent);
									logRowlevel("Row to delete:"+toDeleteRowIdx);
									if (toDeleteRowIdx>-1) {

										//delete the row
										requests.clear();
										requests.add(new Request().setDeleteDimension(
												new DeleteDimensionRequest()
														.setRange(new DimensionRange()
																.setSheetId(data.sourceSheetId)
																.setDimension("ROWS")
																.setStartIndex(toDeleteRowIdx-1) //0-based
																.setEndIndex(toDeleteRowIdx)
														)
												)
										);
										BatchUpdateSpreadsheetRequest body =
												new BatchUpdateSpreadsheetRequest().setRequests(requests);
										BatchUpdateSpreadsheetResponse response =
												data.service.spreadsheets().batchUpdate(spreadsheetId, body).execute();
										logRowlevel("Deletion response:"+response);

									}
								}
							}
						}
						else //if column is not id field but has data
						if(i<length && row[i]!=null) 
						{ 
						   r.add(row[i].toString());
						   logRowlevel("Column "+i+" value: "+row[i].toString());

						}
						else {r.add("");} 
					}
					logRowlevel("Adding row:"+data.currentRow);

					data.rows.add(r);
					logRowlevel("Added row:"+data.currentRow);

					putRow(data.outputRowMeta, row);
					logRowlevel("Putting row:"+data.currentRow);


				}
		} catch (GoogleJsonResponseException e) {
			if (e.getStatusCode()==429) {
				logError(e.getDetails().toString());
				//Too Many Requests
				//{
				//  "code" : 429,
				//  "errors" : [ {
				//    "domain" : "global",
				//    "message" : "Quota exceeded for quota metric 'Write requests' and limit 'Write requests per minute per user' of service 'sheets.googleapis.com' for consumer 'project_number:481134404851'.",
				//    "reason" : "rateLimitExceeded"
				//  } ],
				//  "message" : "Quota exceeded for quota metric 'Write requests' and limit 'Write requests per minute per user' of service 'sheets.googleapis.com' for consumer 'project_number:481134404851'.",
				//  "status" : "RESOURCE_EXHAUSTED"
				//}
			}else {
				throw new KettleException(e.getMessage());
			}

		} catch (Exception e) {
			throw new KettleException(e.getMessage());
		} finally {
			data.currentRow++;
			logRowlevel("Increasing currentRow counter");
		}


      
    return true;
  }
}