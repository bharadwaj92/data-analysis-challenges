## Script for loading the excel spreadsheet into MySQL database

import xlrd
import mysql.connector
from datetime import datetime, date, timedelta
import time
connection = mysql.connector.connect(user=<username>,
                                  password=<password>,
                                  host=<ipaddress>,
                                  database=<defaultdatabase>)
def createTables():
    ordersQry = "CREATE TABLE Orders ( Order_id  integer not null, Status varchar(50), Product_type varchar (50),  Region_name  varchar(20),  Order_amount integer ,  Order_date date, Delivery_date date ,PRIMARY KEY (Order_id));"
    usersQry = "CREATE TABLE Users (User_id integer not null, source varchar(50), date_registered date, PRIMARY KEY(User_id));"
    usersOrders = "CREATE TABLE Users_Orders (Order_id integer, User_id integer); "
    cursor = connection.cursor()
    try:
        cursor.execute(ordersQry)
    except mysql.connector.errors.ProgrammingError:
        qry = "drop table Orders"
        cursor.execute(qry)
        cursor.execute(ordersQry)
    try:
        cursor.execute(usersQry)
    except mysql.connector.errors.ProgrammingError:
        qry = "drop table Users"
        cursor.execute(qry)
        cursor.execute(usersQry)

    try:
        cursor.execute(usersOrders)
    except mysql.connector.errors.ProgrammingError:
        qry = "drop table Users_Orders"
        cursor.execute(qry)
        cursor.execute(usersOrders)
    cursor.close()
    connection.commit()

def insertIntoTable(sheet, tableName):
    cursor = connection.cursor()
    if(tableName == "Orders"):
        trncqry = "truncate Orders"
        query = """INSERT INTO Orders VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(trncqry)
        for r in range(1, sheet.nrows):
            orderid = sheet.cell(r,0).value
            status = sheet.cell(r,1).value
            producttype = sheet.cell(r,2).value
            regionname = sheet.cell(r,3).value
            orderamt = sheet.cell(r,4).value
            orderdate = sheet.cell(r,5).value
            tt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(orderdate) - 2).timetuple()
            t = time.mktime(tt)
            orderdate= time.strftime("%Y/%m/%d", time.gmtime(t))
            deliverydate = sheet.cell(r,6).value
            tt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(deliverydate) - 2).timetuple()
            t = time.mktime(tt)
            deliverydate= time.strftime("%Y/%m/%d", time.gmtime(t))
            values = (orderid,status,producttype,regionname,orderamt,orderdate,deliverydate)
            cursor.execute(query, values)
        cursor.close()
        connection.commit()

    if(tableName == "Users"):
        trncqry = "truncate Users"
        query = """INSERT INTO Users VALUES (%s, %s, %s)"""
        cursor.execute(trncqry)
        for r in range(1, sheet.nrows):
            userid = sheet.cell(r,0).value
            source = sheet.cell(r,1).value
            date_registered = sheet.cell(r,2).value
            tt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(date_registered) - 2).timetuple()
            t = time.mktime(tt)
            date_registered= time.strftime("%Y/%m/%d", time.gmtime(t))
            values = (userid,source,date_registered)
            cursor.execute(query, values)
        cursor.close()
        connection.commit()
    if(tableName == "Users_Orders"):
        trncqry = "truncate Users_Orders"
        query = """INSERT INTO Users_Orders VALUES (%s, %s)"""
        cursor.execute(trncqry)
        for r in range(1, sheet.nrows):
            orderid = sheet.cell(r,0).value
            userid = sheet.cell(r,1).value
            values = (orderid,userid)
            cursor.execute(query, values)
        cursor.close()
        connection.commit()

def loadData():
    createTables()
    book = xlrd.open_workbook("dataanalystFurnitureOnline-sourcedatause.xlsx")
    usersSheet = book.sheet_by_name("Users")
    ordersSheet = book.sheet_by_name("Orders")
    usersOrdersSheet = book.sheet_by_name("Users_Orders")
    insertIntoTable(usersSheet,"Users")
    insertIntoTable(ordersSheet,"Orders")
    insertIntoTable(usersOrdersSheet,"Users_Orders")

if __name__ == "__main__":
    loadData()

