import base64
from datetime import datetime, timedelta, timezone
import requests
import json
from google.cloud import bigquery
import re
from bs4 import BeautifulSoup

url = 'https://www.jma.go.jp/jp/week/319.html'
JST = timezone(timedelta(hours=+9), 'JST')

def insert_bq(days, maxtemp_list, mintemp_list):
    client = bigquery.Client()
    dataset_id = '<replace to your dataset>'
    table_id = '<replace to your table>'
    dataset_ref = client.dataset(dataset_id)
    dataset = client.get_dataset(dataset_ref)

    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)  # API request
    print(dataset_ref)
    print(dataset)
    print(table)
    print(table.schema)
    print(table.description)
    print(table.num_rows)
    
    #insert_table = bigquery.Table(table_id, dataset, schema=table.schema)
    #print(insert_table)

    maxtemp_skip_today = 0
    mintemp_skip_today = 0
    len_maxtemp = len(maxtemp_list)
    if len_maxtemp == 6:
        maxtemp_skip_today = 1
        
    len_mintemp = len(mintemp_list)
    if len_mintemp == 6:
        mintemp_skip_today = 1
    
    print('maxtemp_skip_today:'+str(maxtemp_skip_today))
    print('mintemp_skip_today:'+str(mintemp_skip_today))
    
    dayscount = 0
    for previous in range(-1, -8, -1):
        ## 明日分から向こう7日間、BQにデータをインサートする、最低気温が欠けていた場合。
        if mintemp_skip_today == 1:
            #rows_to_insert = [days[dayscount], previous, maxtemp_list[dayscount],]
            rows_to_insert = [(days[dayscount], previous, maxtemp_list[dayscount],)]
            print(str(rows_to_insert))
            mintemp_skip_today = 0
        else:
            #rows_to_insert = [days[dayscount], previous, maxtemp_list[dayscount], mintemp_list[dayscount]]
            rows_to_insert = [(days[dayscount], previous, maxtemp_list[dayscount], mintemp_list[dayscount])]
            print(str(rows_to_insert))
        dayscount += 1
        
        response = client.insert_rows(table, rows_to_insert)  # API request
        #table.insert_data(rows_to_insert)
        

    

def weatherforecast2bq(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    days = []
    #days.append(datetime.now(JST).strftime("%Y-%m-%d"))
    days.append((datetime.now(JST)+timedelta(days=1)).strftime("%Y-%m-%d"))
    days.append((datetime.now(JST)+timedelta(days=2)).strftime("%Y-%m-%d"))
    days.append((datetime.now(JST)+timedelta(days=3)).strftime("%Y-%m-%d"))
    days.append((datetime.now(JST)+timedelta(days=4)).strftime("%Y-%m-%d"))
    days.append((datetime.now(JST)+timedelta(days=5)).strftime("%Y-%m-%d"))
    days.append((datetime.now(JST)+timedelta(days=6)).strftime("%Y-%m-%d"))
    days.append((datetime.now(JST)+timedelta(days=7)).strftime("%Y-%m-%d"))
    print(days)
    response = requests.get(url)
    
    soup = BeautifulSoup(response.text, 'lxml')
    
    table = soup.find_all("table",attrs={"id":"infotablefont","class":"forecast-top"})
	#print(table)
	#print(type(table))
	#<class 'bs4.element.ResultSet'>

    for a in table:
        row = a.find_all('tr')
        count = 0
        count2 = 0
        maxtemp = []
        mintemp = []
        for targetrow in row:
            count += 1
            if count == 5:
                #print(targetrow)
            	#print(type(targetrow))
                if targetrow.find('th').string == '東京' and re.search('最高', targetrow.find('td').string):
                	#print(targetrow.find('th').string)
                    row_maxtemp = targetrow.find_all("font", attrs={"class":"maxtemp"})
                	#print(row_maxtemp)
                    
                    for a in row_maxtemp:
                    	#print(a.text)
                    	b = a.text.replace("\t","").replace("\n","")
                    	kekka = re.search("\(",b)
                    	#print(kekka)
                    	if kekka != None:
                        	#print(kekka.start())
                        	#print(b[:kekka.start()])
                        	c = b[:kekka.start()]
                        	maxtemp.append(c)
                    	else:
                        	maxtemp.append(b)
            if count == 6:
                if re.search('最低', targetrow.find('td').string):
                    row_mintemp = targetrow.find_all("font", attrs={"class":"mintemp"})
                    #print(row_mintemp)
                    
                    for a in row_mintemp:
                    	#print(a.text)
                    	b = a.text.replace("\t","").replace("\n","")
                    	kekka = re.search("\(", b)
                    	if kekka != None:
                        	#print(kekka.start())
                        	#print(b[:kekka.start()])
                        	c = b[:kekka.start()]
                        	mintemp.append(c)
                    	else:
                        	mintemp.append(b)
    
    if len(maxtemp) !=0 or len(mintemp) != 0:
        print('maxtemp:'+str(maxtemp))
        print('mintemp:'+str(mintemp))
        insert_bq(days, maxtemp, mintemp)

