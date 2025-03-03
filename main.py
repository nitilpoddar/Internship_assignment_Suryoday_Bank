import pyodbc
from typing import Annotated
from fastapi import FastAPI, Query, Depends
from pydantic import BaseModel, field_validator
from enums.enums import Gender, Course, Subjects
import re
import pandas as pd
import numpy as np
import logging as log

#for logging setup
log.basicConfig(level=log.INFO, format="%(asctime)s - %(levelname)s - %(message)s")



########################        SQL CONNECTION        ########################
CONNECTION_STRING = r"DRIVER={SQL Server};SERVER=HP;DATABASE=CollegeAdmission;Trusted_Connection=yes;"

def get_cursor():
    conn = pyodbc.connect(CONNECTION_STRING)
    try:
        cursor = conn.cursor()
        yield cursor
    except Exception as e:
        print("Error in getting cursor: ", e)
    finally:
        cursor.close()
        conn.close()


# using pydantic to create model for the student data 

class Student(BaseModel):
    name: Annotated[str, Query(max_length=50)] = ""  #this is a simple check for ensuring that the length doesnot exceed 50 characters
    age: int
    gender: Gender #this i am using the enum class Gender to ensure that gender fals in the given categories
    marksheet: dict[str, int] #marksheet is a dictionary of subjects and their marks 
    qualifying_result: dict[ str , bool] | None = None
    desired_course: Course

    #-----------------!! DESIRED COURSE !!-----------------#
    @field_validator("desired_course", mode="before")
    @classmethod
    def validate_desired_course(cls, value):
        for member in Course:
            if member.value == value:
                return Course(member).value
        raise ValueError("Invalid Course")
    

    #-----------------!! AGE VALIDATION !!-----------------#
    @field_validator("age", mode="before")
    @classmethod
    def validate_age(cls, value):
        if value < 17 or value > 25:
            raise ValueError("Your age should be between 17 and 25")
        log.info("AGE VALIDATED")
        return value
    
    #-----------------!! NAME validation !!-----------------#

    #moved the name validation part here because of code confusion         !!!!!!!!!!!!!!!!!!!! ----name validation---- !!!!!!!!!!!!!!!!!!!!
    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, value):
        
        if not re.search(r'^[A-Za-z][A-Za-z\s]*$', value):
            raise ValueError("LINE 41 Invalid Name or problem with name")
        log.info("NAME VALIDATED")
        return value.upper()
    

    
    #-----------------!! MARKSHEET validation !!-----------------#

    @field_validator("marksheet", mode="before")
    @classmethod
    def validate_marksheet(cls, value):

        if len(value) != 6:
            raise ValueError("Invalid number of subjects: Please provide marks for 6 subjects")
        
        student_subjects = {k.upper(): val for k, val in value.items()}
        for k,v in student_subjects.items():
            if not re.search(r'^[A-Za-z][A-Za-z0-9\s]*$', k):
                raise ValueError("Invalid Subject Name")
            if v < 0 or v > 100:
                raise ValueError("Invalid Marks: Marks should be between 0 and 100")
        log.info("MARKSHEET VALIDATED")
        return {key.upper(): val for key, val in value.items()}

        
    #----------------Gender validation----------------#
        
    @field_validator("gender", mode="before")
    @classmethod
    def check_gender(cls, value):
        try:
            return Gender(value.upper())
        except ValueError:
            raise ValueError("LINE 55 Invalid Gender")
    
    @field_validator("qualifying_result", mode="before")
    @classmethod
    def check_qualifying_result(cls, value):
        if value is not None:
            return {k.upper(): v for k, v in value.items()}
        return value
    
    

        

        
app = FastAPI() 



#-----------------!! main validation endpoint !!-----------------#
# this endpoint will be used to validate the student data
@app.post("/")
async def Validate_student(student: Student, cursor = Depends(get_cursor)):
    student_dataframe = pd.DataFrame([student.model_dump()])
        
    #-----------------!! DESIRED COURSE validation !!-----------------#
    
    conn = cursor.connection

    try:
        cursor.execute("SELECT BRANCH_NAME FROM BRANCH ORDER BY ID;")
        branch_names = cursor.fetchall()
        branch_names = [branch[0] for branch in branch_names]

        print("-------------------->>>>>>>>The desired course is ", student_dataframe["desired_course"][0].value) #THIS is a debug print statement to tackle the insert query


        if student_dataframe["desired_course"][0] not in branch_names:
            return {"message": "Your desired course is not available"}
    except Exception as e:
        return {"message": e}
    
    log.info("DESIRED COURSE VALIDATED")
    
    #-----------------!! STUDENT EXAM QUALIFICATION VALIDATION validation FOR DESIRED COURSE !!-----------------#
    
    try:
        cursor.execute("""
                       SELECT QF.EXAM_NAME 
                        FROM DEGREE D 
                        INNER JOIN QUALIFY_EXAM QF ON D.QUALIFY_ID = QF.ID
                        INNER JOIN BRANCH B ON B.DEGREE_ID = D.ID

                        WHERE B.BRANCH_NAME = ?;""", student_dataframe["desired_course"][0])
        exam_name = cursor.fetchone()[0]
        print("THE EXAM NAME IS: ", exam_name)

        qualify_exam_keys = [k.upper() for k in student_dataframe["qualifying_result"][0].keys()]

        for m in student_dataframe["qualifying_result"][0].keys():
            print("THE KEYS ARE: ", m)
        
        log.info("QUALIFY EXAM KEYS FETCHED")

        print("THE QUALIFY EXAM KEYS ARE: ", qualify_exam_keys)
        

        #-------------------------- CODE WORKING FINE TILL HERE --------------------------#
        
        if not exam_name == 'NONE':
            
            if exam_name not in qualify_exam_keys:
                return {"message": f"You have not qualified the {exam_name} exam for the desired course LINE 134"}
            elif student_dataframe["qualifying_result"][0][exam_name] == False:
                return {"message": f"You have not qualified the {exam_name} exam for the desired course LINE 136"}
        
        
    except Exception as e:
        return {"message": str(e)+ "LINE 154"}
    

    #-----------------!! STUDENT qualification percentage validation FOR DESIRED COURSE !!-----------------#

    student_avg = int(np.mean(list(student_dataframe["marksheet"][0].values())))
    student_dataframe["average"] = student_avg

    try:
        cursor.execute("""
                       SELECT B.MARKS_PERCENT 
                        FROM DEGREE D 
                        INNER JOIN QUALIFY_EXAM QF ON D.QUALIFY_ID = QF.ID
                        INNER JOIN BRANCH B ON B.DEGREE_ID = D.ID

                        WHERE B.BRANCH_NAME = ?;""", student_dataframe["desired_course"][0])
        req_avg = int(cursor.fetchone()[0])

        if student_avg < req_avg:
            return {"message": f"Your average is {student_avg} and the required average is {req_avg}. You are not eligible for the desired course"}
        

    
    
    except Exception as e:
        return {"message": str(e)+ "LINE 178"}

    log.info("RECOMMENDED COURSES FETCHED")       #so the course is workig till here

    #-----------------!! Getting recommended courses !!-----------------#
    recommended = []
    try:
        # this is a debugging print function to check if the marksheet has correctness in it
        for _ in student_dataframe["marksheet"][0].keys():
            print("THE KEYS ARE: ", _)

        PLACEHOLDER = ','.join('?' for _ in student_dataframe["marksheet"][0].keys())

        #a debug print to check the placeholder
        print("THE PLACEHOLDER IS: ", PLACEHOLDER)

        #ANOTHER DEBUG PRINT STATEMENT TO CHECK THE PLACEHOLDER VALUES FOR THE QUERIES

        XX = [student_avg] + [sub for sub in student_dataframe["marksheet"][0].keys()]

        for _ in XX:
            print("THE PLACEHOLDER VALUES ARE: ", _)

        cursor.execute(f"""
                        SELECT B.BRANCH_NAME, B.MARKS_PERCENT, QF.EXAM_NAME
                        FROM DEGREE D
                        INNER JOIN BRANCH B ON B.DEGREE_ID = D.ID
                        INNER JOIN QUALIFY_EXAM QF ON D.QUALIFY_ID = QF.ID
                        INNER JOIN BRANCH_SUBJECT BS ON B.ID = BS.BRANCH_ID
                        INNER JOIN SUBJECT S ON S.ID = BS.SUBJECT_ID
                        WHERE B.MARKS_PERCENT <= ? AND S.SUBJECT_NAME IN ({PLACEHOLDER})
                        GROUP BY B.ID, B.BRANCH_NAME, B.MARKS_PERCENT, QF.EXAM_NAME
                        HAVING COUNT(*) = (
                            SELECT COUNT(*) 
                            FROM BRANCH_SUBJECT BS2 
                            WHERE BS2.BRANCH_ID = B.ID) 
                       """, *XX )
        
        
        for tup in cursor.fetchall():
            for vv in tup:
                print(f"{vv}", end = " ")
            print() #debug print to check the recommended courses
        
        for row in cursor.fetchall():
            branch_name, marks_percent, exam = row

            if exam == 'NONE' or (exam in student_dataframe["qualifying_result"][0].keys() and student_dataframe["qualifying_result"][0][exam] == True):
                recommended.append({
                    "branch": branch_name,
                    "marks_percent": marks_percent,
                    "exam": exam
                })
        log.info("RECOMMENDED COURSES FETCHED") #this is the last log statement which is working fine
    except Exception as e:
        return {"message": str(e)+ "LINE 222"}
    
    try:
        #testing this try block for insertion of data into the database
        log.info("INSERTING STUDENT DATA INTO DATABASE BEGINS")
        insert_data = [student_dataframe["name"][0]] + [int(student_dataframe["age"][0])] + [student_dataframe["desired_course"][0].value]
        for _ in insert_data:
            print("THE INSERT DATA IS: ", _)
        
        # print("The student dataframe is ", student_dataframe.to_string())
        cursor.execute("""INSERT INTO Student (NAME, AGE, DESIRED_COURSE) VALUES ('TEST', 20, 'COMPUTER SCIENCE ENGINEERING');""")

        #code working till here ???????????????????????????????????????????????
        cursor.execute("""
            INSERT INTO Student (NAME, AGE, DESIRED_COURSE)
            VALUES (?, ?, ?);
            SELECT SCOPE_IDENTITY() AS student_id;
        """, *insert_data)

        log.info("INSERTING STUDENT DATA INTO DATABASE COMPLETED")

        conn.commit()

        student_id = cursor.fetchone()[0]
        print("THE STUDENT ID IS: ", student_id)

        log.info("STUDENT ID FETCHED")

        

        
        for subject, mark in student_dataframe["marksheet"][0].items():
            cursor.execute("SELECT ID FROM SUBJECT WHERE SUBJECT_NAME = ?;", subject)
            subject_id = cursor.fetchone()[0]
            cursor.execute("""
                INSERT INTO StudentSubject (STUDENT_ID, SUBJECT_ID, MARKS)
                VALUES (?, ?, ?);
            """, (student_id, subject_id, mark))
        conn = cursor.connection
        conn.commit()
        
        log.info("STUDENT ENTRY ADDED TO DATABASE")
        
        recommended = [{"branch": student_dataframe["desired_course"][0], "marks_percent": req_avg, "exam": exam_name}]
        return {"message": "You are eligible for the desired course", "recommended_courses": recommended}
    
    except Exception as e:
        return {"message": str(e)}