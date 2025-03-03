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


# using pydantic to create model for the student data 

class Student(BaseModel):
    name: Annotated[str, Query(max_length=50)] = ""  #this is a simple check for ensuring that the length doesnot exceed 50 characters
    age: int
    gender: Gender #this i am using the enum class Gender to ensure that gender fals in the given categories
    marksheet: dict[str, int] #marksheet is a dictionary of subjects and their marks 
    qualifying_result: dict[ str , bool] | None = None
    desired_course: Course

    #moved the name validation part here because of code confusion         !!!!!!!!!!!!!!!!!!!! ----name validation---- !!!!!!!!!!!!!!!!!!!!
    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, value):
        
        if not re.search(r'^[A-Za-z][A-Za-z\s]*$', value):
            raise ValueError("LINE 41 Invalid Name or problem with name")
        return value.upper()
    
    #-----------------!! MARKSHEET validation !!-----------------#

    @field_validator("marksheet", mode="before")
    @classmethod
    def validate_name(cls, value):

        if len(value) != 6:
            raise ValueError("Invalid number of subjects: Please provide marks for 6 subjects")
        
        student_subjects = {k.upper(): val for k, val in value.items()}
        for k,v in student_subjects:
            if not re.search(r'^[A-Za-z][A-Za-z0-9\s]*$', k):
                raise ValueError("Invalid Subject Name")
            if v < 0 or v > 100:
                raise ValueError("Invalid Marks: Marks should be between 0 and 100")
        
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

    print(student_dataframe["marksheet"])
    # return {"message": "Student data validated successfully"}

    #-----------------!! AGE validation !!-----------------#
    try:
        if student_dataframe["age"] < 17 or student_dataframe["age"] > 25:
            return {"message": "Student age should be between 17 and 25"}
    except Exception as e:
        return {"message": e}
    
    #-----------------!! DESIRED COURSE validation !!-----------------#
    
    try:
        cursor.execute("SELECT DISTINCT(BRANCH_NAME) FROM BRANCH ORDER BY ID;")
        branch_names = cursor.fetchall()
        branch_names = [branch[0] for branch in branch_names]

        if student_dataframe["desired_course"][0] not in branch_names:
            return {"message": "Your desired course is not available"}
    except Exception as e:
        return {"message": e}
    
    