from mysql import connector
import pandas as pd
import datetime as dt


#region connecting sql to Python:
connection=connector.connect(
 host='localhost',
 user='root',
 password='1234'
 )
cursor=connection.cursor()
#endregion

#region creation of database and tables:

db_cr = 'create database if not exists GlobalElectronics1'
cursor.execute(db_cr)

db = 'use GlobalElectronics1'
cursor.execute(db)


cust_cr = '''create table if not exists Customers (Customer_key int PRIMARY KEY,Gender Varchar(20), 
Name varchar(255),City varchar(255),State_code varchar(255), state  varchar(255), 
Zip_Code varchar(255),Country varchar(255),Continent varchar(255),Birthday datetime)'''

cursor.execute(cust_cr)

prod_cr = '''create table if not exists Products (Product_key int PRIMARY KEY, Product_Name Varchar(255), 
Brand varchar(255),Colour varchar(255),Unit_cost_USD float, Unit_Price_USD float, 
Subcategory_Key int,Subcategory varchar(255),Category_Key int,Category varchar(255) )'''
cursor.execute(prod_cr)

sales_cr='''create table if not exists Sales (Order_No int, Line_Item int, Order_Date datetime, 
Delivery_Date datetime,Customer_Key int,Store_Key int,Product_Key int,Quantity int,Currency_Code varchar(255))'''
cursor.execute(sales_cr)

exch_cr='''create table if not exists ExchangeRates (Date datetime,Currency varchar(255),Exchange float) '''
cursor.execute(exch_cr)

store_cr='''create table if not exists Stores (Store_Key int PRIMARY KEY, Country varchar(255), 
State varchar(255), Square_Meters float, Open_Date datetime)'''
cursor.execute(store_cr)
#endregion


#region Customer Dataframe and insert to Database

# Cleaning of customer dataset:

df_cust = pd.read_csv(r"D:\Python Projects\DATA SPARK\DATA\Customers.csv", encoding='latin')
df_cust.isna().sum()
df_cust.fillna({'State Code': 'NAP'}, inplace=True)
df_cust['Birthday'] = pd.to_datetime(df_cust['Birthday'])
df_cust['Birthday']=df_cust['Birthday'].dt.strftime('%Y-%m-%d %H:%M:%S')

#converting Dataframes to List of Tuples
cust_info = [tuple(data) for data in df_cust.values]

q='select  customer_key from Customers'

cursor.execute(q)
cuskey = []

for rows in cursor:
    cuskey.append(rows)

if len(cuskey) == 0:
    cus_ins = 'insert into customers values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    cursor.executemany(cus_ins, cust_info)

connection.commit()

#endregion


#region Products Dataframe and insert to database


df_prod=pd.read_csv(r"D:\Python Projects\DATA SPARK\DATA\Products.csv")
df_prod['Unit Cost USD']=df_prod['Unit Cost USD'].replace(r"[\$,]","",regex=True).astype(float)
df_prod['Unit Price USD']=df_prod['Unit Price USD'].replace(r"[\$,]","",regex=True).astype(float)


#converting Dataframes to List of Tuples
prod_info = [tuple(data) for data in df_prod.values]

pq = 'select product_key from products'
cursor.execute(pq)
pql = [i for i in cursor]

if len(pql)  == 0:
    prod_in = 'insert into Products values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    cursor.executemany(prod_in, prod_info)

connection.commit()
#endregion


#region Sales Dataframe and insert to 
df_sales=pd.read_csv(r"D:\Python Projects\DATA SPARK\DATA\Sales.csv")
df_sales.fillna(method='bfill', inplace=True)
df_sales['Order Date'] = pd.to_datetime(df_sales['Order Date'])
df_sales['Order Date'] = df_sales['Order Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
df_sales['Delivery Date'] = pd.to_datetime(df_sales['Delivery Date'])
df_sales['Delivery Date'] = df_sales['Delivery Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
sales_info =  [tuple(data) for data in df_sales.values]
sl_qr = 'select order_no from sales'
cursor.execute(sl_qr)
sl_lt = [i for i in cursor]
if len(sl_lt) == 0:
    sl_ins = 'insert into sales values(%s, %s, %s, %s, %s, %s, %s, %s, %s)'
    cursor.executemany(sl_ins, sales_info)
connection.commit()
#endregion

#region stores dataframe

df_store=pd.read_csv(r"D:\Python Projects\DATA SPARK\DATA\Stores.csv")
df_store.fillna(method='ffill', inplace=True)
df_store['Open Date'] = pd.to_datetime(df_store['Open Date'])
df_store['Open Date'] = df_store['Open Date'].dt.strftime('%Y-%m-%d %H:%M:%S')

str_info = [tuple(i) for i in df_store.values]

str_qr = 'select store_key from stores'
cursor.execute(str_qr)
str_lt = [i for i in cursor]

if len(str_lt) == 0:
    str_ins = 'insert into stores values(%s, %s, %s, %s, %s)'
    cursor.executemany(str_ins, str_info)
connection.commit()

#endregion


#region exchange rate dataframe and insert to db
df_ex=pd.read_csv(r"D:\Python Projects\DATA SPARK\DATA\Exchange_Rates.csv")
df_ex['Date'] = pd.to_datetime(df_ex['Date'])
df_ex['Date'] = df_ex['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')

ex_info = [tuple(j) for j in df_ex.values]

ex_qr = 'select currency from exchangerates'
cursor.execute(ex_qr)
ex_lt = [i for i in cursor]
if len(ex_lt) == 0:
    ex_ins = 'insert into exchangerates values(%s, %s, %s)'
    cursor.executemany(ex_ins, ex_info)
connection.commit()
#endregion