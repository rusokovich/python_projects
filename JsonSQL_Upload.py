import pandas as pd
import json
import pandasql as ps

#Load JSON data


with open('C:\path\\data.json') as file: json_data = json.load(file)

#json_data
#json_data.keys()
#test=json_data['applications']

#print(type(json_data))

#
   
#print(type(df_applications))

#print(df_applications.head())

#df_applications = pd.json_normalize(json_data, record_path="applications")
#df_applications

# Here I set up the base dataframe with all the keys which happen to be the tables
df = pd.json_normalize(json_data,max_level=0)    
print(df.head())

print("-------")


#
df_applications = pd.json_normalize(df['applications'],max_level=1)    
print(df_applications.head())

print("-------")

df_customers = pd.json_normalize(df['customers'],max_level=1).drop_duplicates().set_index('customer_id')  
print(df_customers.head())


print("-------")

df_stores = pd.json_normalize(df['stores'],max_level=1).drop_duplicates().set_index('store')    
print(df_stores.head())

print("-------")

df_marketing = pd.json_normalize(df['marketing'],max_level=1).drop_duplicates().set_index('id')      
print(df_marketing.head())

print("-------")

query = """

with stores AS (
SELECT DISTINCT
store
FROM df_applications
),
dates AS (
SELECT DISTINCT
date(submit_date) date_dt
FROM df_applications
),
stores_dates AS (
SELECT
store,
date_dt
FROM dates,stores
),
summary as (
SELECT
/*Mapping*/
  sd.store,
  sd.date_dt,
/*KPIs*/
  SUM(CASE WHEN da.dollars_used <> 'NaN' THEN da.dollars_used ELSE NULL END)  total_dollars_used,
  SUM(CASE WHEN da.approved_amount <> 'NaN' THEN da.approved_amount ELSE NULL END)  total_dollars_approved,
  SUM(CASE WHEN da.approved <> 'NaN' THEN da.approved ELSE NULL END)  total_applications_approved,
  COUNT(dc.campaign) total_campaigns,
  COUNT(application_id) total_applications
FROM stores_dates sd
LEFT JOIN df_applications da ON date(da.submit_date)=sd.date_dt AND da.store = sd.store
LEFT JOIN df_customers dc on da.customer_id=dc.customer_id  
GROUP BY 1,2
ORDER BY 2
)
SELECT *,
AVG(total_dollars_used) OVER (PARTITION BY store ORDER BY date_dt,store ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) rolling_dollars_used_avg_30days
FROM summary
"""


result = ps.sqldf(query,globals())
print (result)
