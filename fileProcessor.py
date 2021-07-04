import json

import boto3
import os 
import tempfile
from boto3 import client
import botocore
import io
import pprint
import zipfile
import json
from _io import BytesIO
import logging
from pathlib import Path
from datetime import datetime,timedelta
import urllib3



http =urllib3.PoolManager()
logger =logging.getLogger('lamda_function')
logger.setLevel(logging.INFO)

LISTING_LAMDA_URL="https://kf7c0wirn5.execute-api.us-east-2.amazonaws.com/default/fileListingLamda"
SOURCE_BUCKET="source240819960"
KEY="config.txt"

s3Res = boto3.resource('s3')

s3 = boto3.client('s3')

#SQS=boto3.resource('sqs',endpoint_url="")

def lambda_handler():
    #file_processor()
    s3R=s3Res.Object(SOURCE_BUCKET, KEY)
    print(s3R)
    pr=ProductionReporter()
    cv=ControlDateProcessor(s3R,pr)
    cv.process()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    

def file_processor():
    s3Obj = s3Res.Object(SOURCE_BUCKET, KEY)
    print(s3Obj.get()['Body'].read().decode("utf-8"))
    body = json.loads(s3Obj.get()['Body'].read().decode("utf-8"))
    #check if the date is todays date nd its status is pending 
    #check id the record file is present in s3 if not check if header matches with the files listed and create a record file accodingly
    if body["metadata"]["status"]=="PENDING":
        print("a")
        
def increment_date(dcFile):    
    date=datetime.strptime(dcFile["currentDate"],"%Y-%m-%d").date()
    dcFile["currentDate"]=str(date+timedelta(days=1))
    print("date Incremented")
        
def invoke_listing_lamda(query):
   return query;
   # print(LISTING_LAMDA_URL+"?"+query)
    #resp=http.request("GET",LISTING_LAMDA_URL+"?"+query,timeout=5)
   # return resp.data.decode("utf-8")


class Processor:
    def __init__(self, s3obj,reporter):
            self.s3obj=s3obj
            self.reporter=reporter
            
        

class ControlDateProcessor(Processor):
    def __init__(self,s3obj,reporter): #action need to be passed later
        super().__init__(s3obj,reporter)
        self.file_processes =[]
                
    
    def process(self):
    #logger. info("Validating neade
        self.file_processes=[]
        try:
            self.controlFile = json.loads(self.s3obj.get()['Body'].read().decode("utf-8"))
            #could be an error while geting file
            self.currentDate= self.controlFile['metadata']['currentDate']
            self.status=self.controlFile["metadata"]["status"]
            print("control file:-",self.controlFile)
            #check if the date is todays date nd its status is pending 
    #        check id the record file is present in s3 if not check if header matches with the files listed and create a record file accodingly
            #print("headervalidation",totalRecordcount)
            proceed = True
            
                #proceed = validateHeaders(currentDate)    
            if self.status =="PENDING":
                for file in self.controlFile["metadata"]["fileNameList"]:
                    print("in header for loop ",file["prefix"])
                    fv = FileListingProcessor(self.s3obj,file,self)
                    fv.process()
                    self.file_processes.append(fv)#added the Filevatidator-o0jefct having the- bocketCName and 01r oath to
            else :   
                print("needs to be implemented")     
        except Exception as e:
            print(e)
            return False
        return True
    ##=final block if an exception occured close all and get back to the previous stage
    
    
def  update_source_config_file(self,valueAdded):
        r = s3.put_object(Body=json.dumps(valueAdded),Bucket=SOURCE_BUCKET, Key=KEY)
        print(r)

    
def get_updated_obj(date,status):
       return {"currentDate":date,
       "status":"status"
       }

        
class FileListingProcessor(Processor):
    def __init__(self,s3Obj,fileMetadata,control_processor) :
        super().__init__(s3Obj,control_processor.reporter)
        self.fileMetadata=fileMetadata
        self.control_processor = control_processor
        

    def process(self):
        if self.fileMetadata["status"] != "PROCESSED":
            ok=True;            
        print("validating file:",self.fileMetadata)
        try:
            reqParam=self.form_expression()
            self.httpResp=invoke_listing_lamda(reqParam)
            #add some checks if required to check for valid response
            self.reporter.report_files(self)
            # add sqs in reporter to 
        except Exception as e:
            print("error  ",e)
            # if processing fails for that lamda let the reporter know this 
            return False
        return ok
    
    def form_expression(self):
        return self.fileMetadata["prefix"]+self.control_processor.currentDate;
        
class SimpleReporter:
    
    def processCompleted(self, fp):
        logger.info("process completed")
    
    

class ProductionReporter(SimpleReporter):
  
    def report_files(self,file_processor):
        super().processCompleted(file_processor)
        self.sent_to_sqs(file_processor)
        self.update_controlFile(file_processor)
        self.push_controlFile(file_processor)


    def sent_to_sqs(self,file_processor):
        print(f"push response {file_processor.httpResp} for prefix {file_processor.fileMetadata['prefix']} ")
        return True
    
    def push_controlFile(self,fp):
        r = s3.put_object(Body=json.dumps(fp.control_processor.controlFile),Bucket=SOURCE_BUCKET, Key=KEY)

       
    def update_controlFile(self,fp):
        try:
            print("update controlfile"+ str(fp.control_processor.controlFile))
            cf=fp.control_processor.controlFile["metadata"]
            if cf["status"]== "PENDING": 
                cf["status"]="PROCESSING"
                for fileName in cf["fileNameList"]:
                    if fileName["prefix"]==fp.fileMetadata["prefix"] :
                        fileName["status"]="PROCESSED"
                increment_date(cf)
                print("logging control file to check if its updated at an instance level",cf)
            else:
                result = [prefix for prefix in cf["fileNameList"] if prefix["status"]=="PENDING"]
                print("result",result)
                if len(result)==1:
                    cf["status"]="PENDING"
                    for fileName in cf["fileNameList"]:
                        if fileName["prefix"] != "PENDING":
                            fileName["status"]="PENDING"
                    increment_date(cf)
                    print("for last file processing",cf)     
                else :           
                    for file in cf["fileNameList"]:
                        if file["prefix"]==fp.fileMetadata["prefix"]:
                            file["prefix"]="PROCESSED"
                    print("for more than 2 files ",cf)        
        except Exception as e:
            print("parsed records  ",e) 
            
if __name__ == "__main__":
    lambda_handler()

        