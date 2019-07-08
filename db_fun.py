# -*- coding: utf-8 -*-
"""
Created on Mon Jul  8 09:35:20 2019

@author: M06272
"""
import pyodbc #建立DB連接 
import datetime #時間函數
#DB相關函式庫定義及設定

#DB連結設定
def dblink():
    dbconfig={
    "dbip":"192.168.10.161",
    "dbname":"OKMART_SR_M",
    "account":"sa",
    "pwd":"qpass" }
    return  dbconfig

#清空資料表
def truncatetable (table): #清空資料表
    try:
        db=dblink()  
        dbaccess=("DRIVER={SQL Server};SERVER="+db["dbip"]+";DATABASE="+db["dbname"]+";UID="+db["account"]+";PWD="+db["pwd"])  
        conn=pyodbc.connect(dbaccess)
        cursor = conn.cursor()
        delsql='TRUNCATE TABLE  '+' '+table
        cursor.execute(delsql)
        cursor.commit()
        cursor.close()
        return print('已成功清除'+table+'所有資料')
    except:
        cursor.commit()
        cursor.close()
        return print("資料刪除作業失敗，請檢查原因")

#新增資料    
def intotable(df,table): #轉入資料進SQL SERVER,並寫入轉入日期與時間

    try:
        db=dblink()  
        dbaccess=("DRIVER={SQL Server};SERVER="+db["dbip"]+";DATABASE="+db["dbname"]+";UID="+db["account"]+";PWD="+db["pwd"])    
        conn=pyodbc.connect(dbaccess)
        cursor = conn.cursor()
        column=df.columns
        rmax=len(df)
        r=0
        string='?,?'
        tablename=''
    
        for q in df:   
            string=string+',?'
        
        sql='INSERT INTO '+table+'  values('+string+')'    
        for coln in column:
            if tablename=='':
                tablename='df.'+coln+'[r]'
            else:
                tablename=tablename+',df.'+coln+'[r]'  
        tablename=tablename+',str(datetime.date.today()),str(datetime.datetime.now().strftime("%H:%M:%S"))'
        while r<rmax:
            data= eval(tablename)       
            cursor.execute(sql,data)
            r+=1
        cursor.commit() #結束交易
        cursor.close() #斷開DB連接
        return print("已成功轉入資料進"+table+",總共"+str(len(df))+"筆")
    except:
        cursor.commit() #結束交易
        cursor.close() #斷開DB連接
        return print("資料轉入失敗，請檢查原因")
    
#DBMAIL寄發Email        
def  dbmail (profile_name,recipients,copy_recipients,body,body_format,subject):  
    try:
        body="DECLARE @tableHTML NVARCHAR(MAX) "\
        "SET @tableHTML =  "+body
        mail_info={
            "profile_name":profile_name,
            "recipients":recipients ,
            "copy_recipients":copy_recipients,
            "body":body,
            "body_format":body_format,
            "subject":subject
            } 
        db=dblink()  
        dbaccess=("DRIVER={SQL Server};SERVER="+db["dbip"]+";DATABASE="+db["dbname"]+";UID="+db["account"]+";PWD="+db["pwd"])  
        conn=pyodbc.connect(dbaccess)
        cursor = conn.cursor()
        sendmail=body+"EXEC msdb.dbo.sp_send_dbmail  "\
            "@profile_name ="+mail_info["profile_name"]+", " \
            "@recipients = "+mail_info["recipients"]+",  " \
            "@copy_recipients = "+mail_info["copy_recipients"]+",  " \
            "@body = @tableHTML ,  " \
            "@body_format = "+mail_info["body_format"]+",  " \
            "@subject = "+mail_info["subject"]+" ;  "
     
        cursor.execute(sendmail)
        cursor.commit()
        cursor.close()
        return print("信件成功寄出")
    except:
        cursor.commit()
        cursor.close()  
        return print("信件寄送失敗!原語法為：\n"
                     ""+sendmail)
        

        
        
        
