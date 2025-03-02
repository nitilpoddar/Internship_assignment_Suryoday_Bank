import pyodbc
from typing import Annotated
from fastapi import FastAPI, Query, Depends
from pydantic import BaseModel, field_validator
from enums.enums import Gender, Course, Subjects
import re
import pandas as pd
import numpy as np



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

# def execute_query(query: str, cursor):
#     cursor.execute(query)
#     cursor.commit()
#     result = cursor.fetchall()
#     return result


        
    




# using pydantic to create model for the student data 

class Student(BaseModel):
    name: Annotated[str, Query(max_length=50)] = ""  #this is a simple check for ensuring that the length doesnot exceed 50 characters
    age: int
    gender: Gender #this i am using the enum class Gender to ensure that gender fals in the given categories
    marksheet: dict[str, int] #marksheet is a dictionary of subjects and their marks 
    qualifying_result: dict[ str , bool] | None = None
    desired_course: Course


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

def validate_name(name):
    return bool(re.search(r'^[A-Z][A-Z\s]*$', name))

#-----------------!! main validation endpoint !!-----------------#
# this endpoint will be used to validate the student data
@app.post("/")
async def Validate_student(student: Student, cursor = Depends(get_cursor)):
    s_data = pd.DataFrame([student.model_dump()])

    try:
        # validation for name
        name = s_data["name"].apply(lambda x: re.sub(r'\s+', ' ', x.strip()).upper())
        
        if not validate_name(name[0]):
            raise Exception("LINE 82Invalid Name")
        #validatio for age
        if s_data["age"][0] < 17 or s_data["age"][0] > 25:
            raise Exception("LINE 85Invalid Age")
        
        #validation for proper subjexts
        if( len(student.marksheet.keys()) > 6 or len(student.marksheet.keys()) < 6):
            raise Exception("LINE 89 Invalid Count of subjects: 6 subjects are required")
         
        subject_list = {Subject.value for Subject in Subjects }

        # print(subject_list)
        student_subjects = set(student.marksheet.keys())
        invalid_subjects = student_subjects - subject_list
        if student_subjects - subject_list:
            raise Exception(f"LINE 97Invalid Subjects : {invalid_subjects}")
        
        #validationb for marks
        s_marks = pd.DataFrame([student.marksheet])
        if s_marks.applymap(lambda x: x < 0 or x > 100).any().any():
            raise Exception("LINE 102Invalid Marks: Marks should be between 0 and 100")
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        #validation for qualifying exam  result and checking if the student can take admission in the desired course
        # cursor = get_cursor()
        

        
        #i will fetch the list of courses from the database and check if the student's desired course is in it or not
        # try:
        #     BRANCHES = cursor.execute("SELECT BRANCH_NAME FROM BRANCH ORDER BY ID;")
        #     BRANCHES = BRANCHES.fetchall()
        #     #this is just a check to see branches is being extracted succesfully
        #     # for branch in BRANCHES:
        #     #     print(branch[0], end="\n")
        #     # print("BRANCH PRINTED SUCCESSFULLY")

        # except Exception as e:
        #     print("Error while getting branches: message: ", e)


        course = [member.value for member in Course if member.value == student.desired_course]
        if len(course) == 0:
            raise Exception("LINE 137 Invalid Desired Course")
        # return {"Message": f"{course[0]}"}

        cursor.execute(f"""
        SELECT SUBJECT_NAME 
        FROM SUBJECT
        INNER JOIN BRANCH_SUBJECT ON SUBJECT.ID = BRANCH_SUBJECT.SUBJECT_ID
        INNER JOIN BRANCH ON BRANCH.ID = BRANCH_SUBJECT.BRANCH_ID
        WHERE BRANCH_NAME = '{course[0]}';
        """)
        
        subjects_in_course = cursor.fetchall()
        for sub in subjects_in_course:
            if not sub in subjects_in_course:
                print(subjects_in_course)
                raise Exception(f"LINE 153 Invalid Subjects: Required subjects for this {student.desired_course} do not match with the subjects provided")

        return {"Message": "seems fine now and subjects also match with the desired course"}


    except Exception as e:
        return {"Error Message: ": f'{str(e)} line 125'}
        
    


