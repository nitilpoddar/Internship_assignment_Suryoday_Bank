import pyodbc
from typing import Annotated
from fastapi import FastAPI, Query
from pydantic import BaseModel, field_validator
from enums import Gender, Course, Subjects
import re
import pandas as pd
import numpy as np

# using pydantic to create model for the student data 

class Student(BaseModel):
    name: Annotated[str, Query(max_length=50)] = ""  #this is a simple check for ensuring that the length doesnot exceed 50 characters
    age: int
    gender: Gender #this i am using the enum class Gender to ensure that gender fals in the given categories
    marksheet: dict[str, int]
    qualifying_result: dict[ str , bool]
    desired_courses: Course

    @field_validator("gender", mode="before")
    @classmethod
    def check_gender(cls, value):
        try:
            return Gender(value.upper())
        except ValueError:
            raise ValueError("Invalid Gender")

        
app = FastAPI()

def validate_name(name):
    return bool(re.search(r'^[A-Z][A-Z\s]*$', name))


@app.post("/validate")
async def Validate_student(student: Student):
    s_data = pd.DataFrame([student.model_dump()])

    try:
        # validation for name
        name = s_data["name"].apply(lambda x: re.sub(r'\s+', ' ', x.strip()).upper())
        
        if not validate_name(name[0]):
            raise Exception("Invalid Name")
        #validatio for age
        if s_data["age"][0] < 17 or s_data["age"][0] > 25:
            raise Exception("Invalid Age")
        
        #validation for proper subjexts
        subject_list = {Subject.value for Subject in Subjects }
        
        #validation for marks
        s_marks = pd.DataFrame([student.marksheet])
        if s_marks.apply(lambda x: x < 0 or x > 100).any().any():
            raise Exception("Invalid Marks: Marks should be between 0 and 100")
        


        

        
    except Exception as e:
        return {"Error": str(e)}
        
    

@app.get("/validate")
async def Validate_student():
    return {"HELLO": "WORLD"}
