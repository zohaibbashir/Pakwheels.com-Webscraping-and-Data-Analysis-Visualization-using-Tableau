# %%
# !pip install selenium
# !pip install bs4
# !pip install beautifulsoup4
# !pip install chromedriver-autoinstaller

# %%
# pip install mysql-connector-python

# %%
import pandas as pd
import numpy as np
import re
from bs4 import BeautifulSoup
from requests import get
from lxml import html
from selenium import webdriver
import time
import chromedriver_autoinstaller
import mysql.connector
from mysql.connector import Error


chromedriver_autoinstaller.install()  

def removeChar(s,x):
    return s.replace(x, "")

# %%
ders = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}
#user=input("Enter the name of the Car You want to buy:")
#choices=int(input("Enter upto how many Pages you want to receive Information from:"))
st=""
#page=1
user="Yaris"
for i in user.split():
        st+=i + "+"
st = st[:-1]
url=f'https://www.pakwheels.com/used-cars/search/-/?q={st}'
print("Link:",url,"\n\n\n\n")
#url=f'https://www.daraz.pk/catalog/?q={st}&_keyori=ss&from=input&page={page}&spm=a2a0e.home.search.go.35e3493779rCiu'
driver = webdriver.Chrome()
driver.get(url)



html = driver.page_source
parsed_html = BeautifulSoup(html, "html.parser")

driver.close()

# %%


containers = parsed_html.find_all("div", {"class" : "col-md-9 grid-style"})

name=[]
city=[]
info=[]
info1=[]
info2=[]
info3=[]
info4=[]
info5=[]

updated_info=[]
price=[]
price2=[]

certified=[]
managed_by=[]

count=0

for i in range(0,(len(containers))):
    first_product = containers[i]
    car_name=first_product.find('h3').text
    car_name=removeChar(car_name," for Sale")
    name.append(car_name)
#list-unstyled search-vehicle-info fs13
    city_name=first_product.find('ul', class_ = 'list-unstyled search-vehicle-info fs13').text
    city_name = re.sub(r'[^a-zA-Z]', '', city_name)
    city.append(city_name)
#price-details generic-dark-grey
    info_car=first_product.find('ul', class_ = 'list-unstyled search-vehicle-info-2 fs13').text
    count_info=1
    info_car=info_car.split()
    info1.append(info_car[0])#year
    info2.append(info_car[1]+info_car[2])#KMs
    info3.append(info_car[3])#fuel
    info4.append(info_car[4]+info_car[5])#Horsepower
    info5.append(info_car[6])#transmission
    info.append(info_car[0] + " " + info_car[1] + info_car[2] + " " + info_car[3] + " " + info_car[4]+ info_car[5] + " " +info_car[6])

    updated_information=first_product.find('div', class_ = 'pull-left dated').text
    updated_info.append(updated_information)


    prices=first_product.find('div', class_ = 'price-details generic-dark-grey').text
    prices=re.sub(r'\s+', ' ', prices.strip())
    price.append(prices)



    cert=first_product.find(class_ = 'pull-left mr5')
    if(cert):
        certified.append("Certified")
    else:
        certified.append("Not Certified")
    
    manage=first_product.find('span', class_ = 'sold-by-pw')
    if(manage):
        managed_by.append("PakWheels")
    else:
        managed_by.append("Customer")
#sold-by-pw
price_number=[]
for price_num in price:
    price_num = price_num.replace('PKR', '')
    # Extract the number and unit
    number, unit = price_num.split()

    # Convert number to float
    number = float(number)

    # Multiply based on unit
    if unit == 'lacs':
        number *= 100000
    elif unit == 'crore':
        number *= 10000000

    # Append updated price to the new list
    price_number.append(number)



df = pd.DataFrame(list(zip(name, price,price_number,city,info1,info2,info3,info4,info5,certified,managed_by)), columns =['Name', 'PriceInText','Price','City','Year','Distance','Fuel','HorsePower','Transmission','Certified','Managed By'])

df.to_csv(r'result.csv', index = False)
df.head()

# %%
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

        
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


pw="admin"
connection = create_server_connection("localhost", "root", pw)
create_database_query="Create Database PakWheels"
create_database(connection,create_database_query)
create_table="""
CREATE TABLE PKWH (
    Name VARCHAR(100),
    PriceInText VARCHAR(20),
    Price FLOAT,
    City VARCHAR(50),
    Year INT,
    Distance VARCHAR(20),
    Fuel VARCHAR(20),
    HorsePower VARCHAR(20),
    Transmission VARCHAR(20),
    Certified VARCHAR(20),
    ManagedBy VARCHAR(50)
);

"""
db="PakWheels"
connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
execute_query(connection, create_table) # Execute our defined query

# %%
def insert_data_into_mysql(dataframe, table_name, connection):
    cursor = connection.cursor()
    try:
        for _, row in dataframe.iterrows():
            sql_query = f"SELECT COUNT(*) FROM {table_name} WHERE Name = %s AND PriceInText = %s AND Price = %s AND City = %s AND Year = %s AND Distance = %s AND Fuel = %s AND HorsePower = %s AND Transmission = %s  AND Certified = %s AND ManagedBy = %s"
            values = (row['Name'], row['PriceInText'], row['Price'], row['City'], row['Year'], row['Distance'], row['Fuel'], row['HorsePower'], row['Transmission'], row['Certified'], row['Managed By'])
            cursor.execute(sql_query, values)
            result = cursor.fetchone()[0]
            if result == 0:
                sql_query = f"INSERT INTO {table_name} (Name, PriceInText, Price, City, Year, Distance, Fuel, HorsePower, Transmission,Certified,ManagedBy) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql_query, values)
        connection.commit()
        print("Data inserted successfully into MySQL table")
    except Error as e:
        print("Error inserting data into MySQL table", e)
    finally:
        cursor.close()

insert_data_into_mysql(df, 'PKWH', connection)

# %%
# import random

# # Generate a large numerical list
# numerical_list = [random.randint(1, 10) for _ in range(100000)]

# # Generate a corresponding text list with 'lac' and 'crore' values
# text_list = ['lac' if random.random() < 0.5 else 'crore' for _ in range(10)]

# def custom_sort(item):
#     number, text = item
#     if text == "lac":
#         return number
#     elif text == "crore":
#         return number * 100

# combined_list = list(zip(numerical_list, text_list))
# sorted_list = sorted(combined_list, key=custom_sort)
# sorted_numerical_list = [item[0] for item in sorted_list]

# print(sorted_list)
import logging

# Set the log file path
filename = 'D:\Programming\Portfolio\Pakwheels\\test_log.log'  # Replace with the desired log file path on Windows

# Logger
print(filename)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(filename)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

def do_logging():
    logger.info("test")


if __name__ == '__main__':
    do_logging()

