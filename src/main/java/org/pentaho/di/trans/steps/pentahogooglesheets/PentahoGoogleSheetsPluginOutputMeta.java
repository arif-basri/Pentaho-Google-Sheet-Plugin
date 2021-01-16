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

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.di.trans.steps.csvinput.CsvInputMeta;
import org.pentaho.di.trans.steps.file.BaseFileField;

import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.security.KeyStore;

import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;


/**
 * Skeleton for PDI Step plugin.
 */

@Step(
	id = "PentahoGoogleSheetsPluginOutputMeta", 
	image = "PentahoGoogleSheetsPluginOutput.svg", 
	name = "PentahoGoogleSheetsPluginOutput.Step.Name", 
	i18nPackageName = "org.pentaho.di.trans.steps.PentahoGoogleSheetsPluginOutput",
	description = "PentahoGoogleSheetsPluginOutput.Step.Name", 	
	categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Output",
	documentationUrl ="https://github.com/jfmonteil/Pentaho-Google-Sheet-Plugin/blob/master/README.md"
	) 
	
@InjectionSupported( localizationPrefix = "PentahoGoogleSheetsPluginOutput.injection.", groups = {"SHEET"} )
public class PentahoGoogleSheetsPluginOutputMeta extends BaseStepMeta implements StepMetaInterface {
	
	private static Class<?> PKG = PentahoGoogleSheetsPluginOutputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
    public  PentahoGoogleSheetsPluginOutputMeta() {
      super(); // allocate BaseStepMeta
	  //allocate(0);
    }

    @Injection( name = "jsonCrendentialPath", group = "SHEET" )
    private String jsonCredentialPath;
 
	@Injection( name = "spreadsheetKey", group = "SHEET" )
    private String spreadsheetKey;
    
	@Injection( name = "worksheetId", group = "SHEET" )
	private String worksheetId;
	
	@Injection( name = "Email", group = "SHEET" )
	private String shareEmail;
	
	@Injection( name = "Domain", group = "SHEET" )
	private String shareDomain;
	
	@Injection( name = "Create", group = "SHEET" )
	private Boolean create; 
	
    @Injection( name = "Append", group = "SHEET" )
	private Boolean append;

    @Injection( name = "ClearBeforeWrite", group = "SHEET" )
    private Boolean clearBeforeWrite;

    @Injection( name = "UpdateKeyField", group = "SHEET" )
    private Boolean updateKeyField;

    @Injection( name = "DeleteRow", group = "SHEET" )
    private Boolean deleteRow;

    @Injection( name = "BulkDeleteRow", group = "SHEET" )
    private Boolean bulkDeleteRow;

    @Override
    public void setDefault() {   
        this.jsonCredentialPath = Const.getKettleDirectory() + "/client_secret.json";
		this.spreadsheetKey = "";
        this.worksheetId = "";  
		this.shareDomain = "";  
		this.shareEmail = ""; 
		this.create=false;
		this.append=false;
		this.clearBeforeWrite=false;
		this.updateKeyField=false;
		this.deleteRow=false;
        this.bulkDeleteRow=false;
    }
		
   /* public String getDialogClassName() {
        return "org.pentaho.di.ui.trans.steps.pentahogooglesheets.PentahoGoogleSheetsPluginOutputDialog";
    }*/
	
    public String getJsonCredentialPath() {
        return this.jsonCredentialPath == null ? "" : this.jsonCredentialPath;
    }

    public void setJsonCredentialPath(String key) {
        this.jsonCredentialPath = key;
    }
	
	public String getSpreadsheetKey() {
        return this.spreadsheetKey == null ? "" : this.spreadsheetKey;
    }

    public void setSpreadsheetKey(String key) {
        this.spreadsheetKey = key;
    }
	
	public String getShareEmail() {
        return this.shareEmail == null ? "" : this.shareEmail;
    }

    public void setShareEmail(String shareEmail) {
        this.shareEmail = shareEmail;
    }
	
	public String getShareDomain() {
		return this.shareDomain == null ? "" : this.shareDomain;
    }

    public void setShareDomain(String shareDomain) {
        this.shareDomain = shareDomain;
    }
	
	public Boolean getCreate() {
        return this.create == null ? false : this.create;
    }

    public void setCreate(Boolean create) {
        this.create = create;
    }

    public void setAppend(Boolean append) {
        this.append = append;
    }
	
	public Boolean getAppend() {
        return this.append == null ? false : this.append;
    }

    public void setClearBeforeWrite(Boolean clearBeforeWrite) {
        this.clearBeforeWrite = clearBeforeWrite;
    }

    public Boolean getClearBeforeWrite() {
        return this.clearBeforeWrite == null ? false : this.clearBeforeWrite;
    }

    public void setUpdateKeyField(Boolean updateKeyField) {
        this.updateKeyField = updateKeyField;
    }

    public Boolean getUpdateKeyField() {
        return this.updateKeyField == null ? false : this.updateKeyField;
    }

    public Boolean getDeleteRow() {
        return this.deleteRow == null ? false : this.deleteRow;
    }

    public void setDeleteRow(Boolean deleteRow) {
        this.deleteRow = deleteRow;
    }

    public Boolean getBulkDeleteRow() {
        return this.bulkDeleteRow == null ? false : this.bulkDeleteRow;
    }

    public void setBulkDeleteRow(Boolean bulkDeleteRow) {
        this.bulkDeleteRow = bulkDeleteRow;
    }



    public String getWorksheetId() {
        return this.worksheetId == null ? "" : this.worksheetId;
    }

    public void setWorksheetId(String id) {
        this.worksheetId = id;
    }

    @Override
    public Object clone() {
        PentahoGoogleSheetsPluginOutputMeta retval = (PentahoGoogleSheetsPluginOutputMeta) super.clone();
        retval.setJsonCredentialPath(this.jsonCredentialPath);
		retval.setSpreadsheetKey(this.spreadsheetKey);
        retval.setWorksheetId(this.worksheetId);
		retval.setCreate(this.create);
		retval.setAppend(this.append);
        retval.setClearBeforeWrite(this.clearBeforeWrite);
        retval.setUpdateKeyField(this.updateKeyField);
        retval.setDeleteRow(this.deleteRow);
        retval.setBulkDeleteRow(this.bulkDeleteRow);
		retval.setShareEmail(this.shareEmail);
	    retval.setShareDomain(this.shareDomain);
        return retval;
    }

    @Override
    public String getXML() throws KettleException {
        StringBuilder xml = new StringBuilder();
        try {         
            xml.append(XMLHandler.addTagValue("jsonCredentialPath", this.jsonCredentialPath));
			xml.append(XMLHandler.addTagValue("worksheetId", this.worksheetId));
			xml.append(XMLHandler.addTagValue("spreadsheetKey", this.spreadsheetKey));
     		xml.append(XMLHandler.addTagValue( "CREATE", Boolean.toString(this.create)));
            xml.append(XMLHandler.addTagValue( "APPEND", Boolean.toString(this.append)));
            xml.append(XMLHandler.addTagValue( "ClearBeforeWrite", Boolean.toString(this.clearBeforeWrite)));
            xml.append(XMLHandler.addTagValue( "UpdateKeyField", Boolean.toString(this.updateKeyField)));
            xml.append(XMLHandler.addTagValue("DeleteRow", Boolean.toString(this.deleteRow)));
            xml.append(XMLHandler.addTagValue("BulkDeleteRow", Boolean.toString(this.bulkDeleteRow)));
			xml.append(XMLHandler.addTagValue("SHAREEMAIL", this.shareEmail));	
            xml.append(XMLHandler.addTagValue("SHAREDOMAIN", this.shareDomain));
        } catch (Exception e) {
            throw new KettleValueException("Unable to write step to XML", e);
        }
        return xml.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        try {
            this.jsonCredentialPath = XMLHandler.getTagValue(stepnode, "jsonCredentialPath");
            this.worksheetId = XMLHandler.getTagValue(stepnode, "worksheetId");
            this.spreadsheetKey = XMLHandler.getTagValue(stepnode, "spreadsheetKey");
			this.create= Boolean.parseBoolean( XMLHandler.getTagValue( stepnode,"CREATE" ));
			this.append= Boolean.parseBoolean( XMLHandler.getTagValue( stepnode,"APPEND" ));
			this.clearBeforeWrite= Boolean.parseBoolean( XMLHandler.getTagValue( stepnode, "ClearBeforeWrite"));
			this.updateKeyField= Boolean.parseBoolean( XMLHandler.getTagValue( stepnode, "UpdateKeyField"));
			this.deleteRow= Boolean.parseBoolean( XMLHandler.getTagValue(stepnode, "DeleteRow"));
            this.bulkDeleteRow= Boolean.parseBoolean( XMLHandler.getTagValue(stepnode, "BulkDeleteRow"));
			this.shareEmail= XMLHandler.getTagValue(stepnode,"SHAREEMAIL" );
            this.shareDomain= XMLHandler.getTagValue(stepnode,"SHAREDOMAIN" );

        } catch (Exception e) {
            throw new KettleXMLException("Unable to load step from XML", e);
        }
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases) throws KettleException {
        try {

            this.jsonCredentialPath = rep.getStepAttributeString(id_step, "jsonCredentialPath");
			this.worksheetId = rep.getStepAttributeString(id_step, "worksheetId");
            this.spreadsheetKey = rep.getStepAttributeString(id_step, "spreadsheetKey");
			this.shareEmail=rep.getStepAttributeString(id_step, "SHAREEMAIL");
			this.shareDomain=rep.getStepAttributeString(id_step, "SHAREDOMAIN");
			this.create=Boolean.parseBoolean( rep.getStepAttributeString( id_step, "CREATE" ));
			this.append=Boolean.parseBoolean( rep.getStepAttributeString( id_step, "APPEND" ));
			this.clearBeforeWrite= Boolean.parseBoolean( rep.getStepAttributeString(id_step, "ClearBeforeWrite"));
			this.updateKeyField= Boolean.parseBoolean( rep.getStepAttributeString(id_step, "UpdateKeyField"));
			this.deleteRow= Boolean.parseBoolean( rep.getStepAttributeString(id_step,"DeleteRow"));
            this.bulkDeleteRow= Boolean.parseBoolean( rep.getStepAttributeString(id_step,"BulkDeleteRow"));

       
        } catch (Exception e) {
            throw new KettleException("Unexpected error reading step information from the repository", e);
        }
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        try {
            rep.saveStepAttribute(id_transformation, id_step, "jsonCredentialPath", this.jsonCredentialPath);
			rep.saveStepAttribute(id_transformation, id_step, "spreadsheetKey", this.spreadsheetKey);
            rep.saveStepAttribute(id_transformation, id_step, "worksheetId", this.worksheetId);
			if(this.shareEmail!=null){
			  rep.saveStepAttribute(id_transformation, id_step, "SHAREEMAIL", this.shareEmail);
			}
			if(this.shareDomain!=null){
			  rep.saveStepAttribute(id_transformation, id_step, "SHAREDOMAIN", this.shareDomain);
			}
            if ( this.create != null ) {
              rep.saveStepAttribute( id_transformation, id_step, "CREATE", this.create );
			}
			if ( this.append != null ) {
              rep.saveStepAttribute( id_transformation, id_step, "APPEND", this.append );
			}
			if (this.clearBeforeWrite != null ) {
			    rep.saveStepAttribute(id_transformation, id_step, "ClearBeforeWrite", this.clearBeforeWrite);
            }
			if (this.updateKeyField != null) {
			    rep.saveStepAttribute(id_transformation, id_step, "UpdateKeyField",this.updateKeyField);
            }
			if (this.deleteRow != null) {
			    rep.saveStepAttribute(id_transformation, id_step, "DeleteRow", this.deleteRow);
            }
            if (this.bulkDeleteRow != null) {
                rep.saveStepAttribute(id_transformation, id_step, "BulkDeleteRow", this.bulkDeleteRow);
            }

        } catch (Exception e) {
            throw new KettleException("Unable to save step information to the repository for id_step=" + id_step, e);
        }
    }

 

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository, IMetaStore metaStore) {
        if (prev == null || prev.size() == 0) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "Not receiving any fields from previous steps.", stepMeta));
        } else {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, String.format("Step is connected to previous one, receiving %1$d fields", prev.size()), stepMeta));
        }

        if (input.length > 0) {
            remarks.add( new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Step is receiving info from other steps!", stepMeta) );
        } else {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "No input received from other steps.", stepMeta));
        }
    }
  
  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans ) {
    return new PentahoGoogleSheetsPluginOutput( stepMeta, stepDataInterface, cnr, tr, trans );
  }
  
  @Override
  public StepDataInterface getStepData() {
    return new PentahoGoogleSheetsPluginOutputData();
  }
}

