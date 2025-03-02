#THIS IS A FILE WHICH I USED TO QUICKLY PRACTICE QUERIES ONTHE DATABASE

import pyodbc

CONNECTION_STRING = r"DRIVER={SQL Server};SERVER=HP;DATABASE=CollegeAdmission;Trusted_Connection=yes;"

try:
    conn = pyodbc.connect(CONNECTION_STRING)
    print("Connection established successfully")
    cursor = conn.cursor()
    print("Cursor created successfully")

    '''query = """CREATE TABLE DEGREE (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        NAME VARCHAR(50) NOT NULL UNIQUE,
        QUALIFY_EXAM VARCHAR(10) DEFAULT ('NA')
    );"""

    cursor.execute(query)
    conn.commit()
    print("Table created successfully")'''

    query = """SELECT BRANCH_NAME FROM BRANCH ORDER BY ID;"""
    cursor.execute(query)
    # conn.commit()
    # print("Data inserted successfully")
    row = cursor.fetchall()
    
    for row in row:
        print(row[0])


except Exception as e:
    print("Error in getting cursor: ", e)

finally:
    if(conn):
        if(cursor):
            cursor.close()
        conn.close()