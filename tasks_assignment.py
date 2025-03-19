""" this is the main app of the Tasks Assignment
"""
import json
import logging
import logging.config
import time
from datetime import datetime, timedelta
from functools import wraps
from typing import List
from uuid import uuid4

import requests
from fastapi import (
    BackgroundTasks,
    FastAPI,
    HTTPException,
    Request,
    Response,
    status,
)
from geopy.distance import geodesic
from pydantic import BaseModel, Field, StrictStr
from pydantic_settings import BaseSettings  # Updated import


class Employee(BaseModel):
    """this is the Employee Data Model

    :param BaseModel: _description_
    :type BaseModel: _type_
    :return: _description_
    :rtype: _type_
    """
    name: StrictStr = Field(...,
                            description="Name of the employee",
                            example="Ahmed Hani")
    lat: StrictStr = Field(...,
                           description="Latitude location of the employee",
                           example="50.71229")
    long: StrictStr = Field(...,
                           description="Lonitude location of the employee",
                           example="4.52529")
    area: StrictStr = Field(...,
                            description="Area/District/Mini-District",
                            example="Rixensart")
    country: StrictStr = Field(...,
                               description="Country",
                               example="BE")
    city: StrictStr = Field(...,
                               description="city",
                               example="Europe/Brussels")
    valid_from: StrictStr = Field(...,
                                  description="Availability start time im 24H format",
                                  example="4:00:00")
    valid_to: StrictStr = Field(...,
                                description="Availability end time im 24H format",
                                example="13:00:00")
    gender: StrictStr = Field(...,
                                description="gender male/female",
                                example="male")


class PatientsRequests(BaseModel):
    """this is the Patients Data Model

    :param BaseModel: _description_
    :type BaseModel: _type_
    :return: _description_
    :rtype: _type_
    """
    name: StrictStr = Field(...,
                            description="Name of the patient",
                            example="Ahmed Hani")
    lat: StrictStr = Field(...,
                           description="Latitude location of the patient",
                           example="50.71229")
    long: StrictStr = Field(...,
                           description="Lonitude location of the patient",
                           example="4.52529")
    area: StrictStr = Field(...,
                            description="Area/District/Mini-District",
                            example="Rixensart")
    country: StrictStr = Field(...,
                               description="Country",
                               example="BE")
    city: StrictStr = Field(...,
                               description="city",
                               example="Europe/Brussels")
    valid_from: StrictStr = Field(...,
                                  description="Availability start time im 24H format",
                                  example="4:00:00")
    valid_to: StrictStr = Field(...,
                                description="Availability end time im 24H format",
                                example="13:00:00")
    gender: StrictStr = Field(...,
                                description="gender male/female",
                                example="male")
    visit_duration: StrictStr = Field(...,
                                      description="duration needed to cover this request in mins",
                                      example="7")
    gender_target: StrictStr = Field(default="any",
                                     description="gender male/female/any",
                                     example="male")


class TasksAssignmentRequest(BaseModel):
    """This is the request expected to be received from the TasksAssignmentRequest endpoint

    :param BaseModel: _description_
    :type BaseModel: _type_
    """
    employees: List[Employee] = Field(...,
                                      description="List of Employees Object to assign them to the requests/patients",
                                      example=[Employee(
                                          **{"name": "Robin Mendoza",
                                             "lat": "50.71229",
                                             "long": "4.52529",
                                             "area": "Rixensart",
                                             "country": "BE",
                                             "city": "Europe/Brussels",
                                             "valid_from": "4:00:00",
                                             "valid_to": "13:00:00",
                                             "gender": "male"
                                             }
                                      )])
    
    requests: List[PatientsRequests] = Field(...,
                                             description="List of Requests Object to be assigned",
                                             example=[PatientsRequests(
                                                **{
                                                "name": "Michael Jordan",
                                                "lat": "50.56149",
                                                "long": "4.69889",
                                                "area": "Gembloux",
                                                "country": "BE",
                                                "city": "Europe/Brussels",
                                                "valid_from": "6:00:00",
                                                "valid_to": "21:00:00",
                                                "gender": "male",
                                                "gender_requirement": "any",
                                                "visit_duration": "7"
                                                }
                                             )])


def log_after_post_success(func):
    """
    Utility function used as a decorator for all of the requests to
    estimate the elapsed time and logging
    :param func:
    :return:
    """

    @wraps(func)
    async def wrapper(req: Request, response: Response, background_tasks=None):
        start_time = time.perf_counter()
        
        if background_tasks is None:
            response = await func(req, response)
        else:
            response = await func(req, response, background_tasks)
            
        elapsed_time = round((time.perf_counter() - start_time) * 1000.0, 3)

        if isinstance(response, dict):
            request_id = response["request_id"]
            request_time = response["request_time"]
        else:
            request_id = response.request_id
            request_time = response.request_time

        logger.info(f'request_id: {request_id} | request_time: {request_time} |  '
                    f'elapsed_time: {elapsed_time}ms: Successful response generation')

        return response

    return wrapper


logging.config.fileConfig(fname='log.conf', disable_existing_loggers=False)
logger = logging.getLogger()


app = FastAPI(
    title="Tasks Assignments Phase 1",
    description=f"This is a Tasks Assignment aaS API 0.0.5",
    version='0.0.5'
)


# re-configuring logging after module import which disrupts other logging
logging.config.fileConfig(fname='log.conf', disable_existing_loggers=False)


# Function to calculate distance between two points using latitude and longitude
def calculate_distance(lat1, lon1, lat2, lon2):
    coord1 = (float(lat1), float(lon1))
    coord2 = (float(lat2), float(lon2))
    return geodesic(coord1, coord2).kilometers


# get real duration using gmaps
def get_duration(lat1, lon1, lat2, lon2) -> float:
    """ calls gmaps API """

    url = 'https://maps.googleapis.com/maps/api/directions/json'
    params = {
        'origin': f'{lat1},{lon1}',
        'destination': f'{lat2},{lon2}',
        'key': 'AIzaSyAA7RHO8PYhmZ8V-wU1JQ88m9Lcl6-4IN4' # TODO secure this!
    }

    response = requests.get(url, params=params, verify=False).json()
    duration_in_seconds = response['routes'][0]['legs'][0]['duration']['value']
    duration_in_mins = duration_in_seconds / 60.0

    print(duration_in_mins)

    return duration_in_mins


def check_request_within_receiver(request_start, request_end, receiver_start, receiver_end, time_to_reach):
    # Convert string times to datetime objects
    request_start_time = datetime.strptime(request_start, "%H:%M:%S").time()
    request_end_time = datetime.strptime(request_end, "%H:%M:%S").time()
    receiver_start_time = datetime.strptime(receiver_start, "%H:%M:%S").time()
    receiver_end_time = datetime.strptime(receiver_end, "%H:%M:%S").time()

    
    # Check if request start and end times are within receiver start and end times
    if receiver_start_time <= request_start_time < receiver_end_time and \
       receiver_start_time < request_end_time <= receiver_end_time:
        
        # Check if current time + 7 hours is within receiver's available time
        current_time = datetime.now()
        future_time = (current_time + timedelta(minutes=time_to_reach))

        # print(future_time)
        # print(receiver_start_time)
        # print(receiver_end_time)
        # exit()

        if future_time.time() <= request_start_time:
            future_time = datetime.strptime(request_start, "%H:%M:%S")

            return True, future_time
        elif future_time.time() <= receiver_end_time:
            return True, future_time
        else:
            return False, None
    else:
        return False, None


def check_gender_requirement(employee_gender: str, request_target: str):
    """ checks if assignment meets the requirements of gender """
    if request_target == 'any':
        return True
    
    return employee_gender == request_target


@app.post("/api/v0/assign-tasks", status_code=200)
@log_after_post_success
async def create_case_task_consolidated_predictions(req: TasksAssignmentRequest, response: Response, 
                                                    background_tasks: BackgroundTasks):
    """this is the endpoint of tasks assignment problem

    :param pred: _description_
    :type pred: TasksAssignmentRequest
    :param response: _description_
    :type response: Response
    :param background_tasks: _description_
    :type background_tasks: BackgroundTasks
    :raises HTTPException: _description_
    :raises HTTPException: _description_
    :raises HTTPException: _description_
    :return: _description_
    :rtype: _type_
    """
    logger.info('calling tasks assignment algorithm')

    response.headers["X-APP-VERSION"] = '0.0.1'

    request_id = str(uuid4())
    request_time = datetime.now().isoformat()

    logger.info(f"request_id: {request_id} | Detecting request fields languages")

    employees = [emp.dict() for emp in req.employees]
    requests_list = [reqs.dict() for reqs in req.requests]

    employees.sort(key=lambda x: datetime.strptime(x['valid_from'], '%H:%M:%S'))

    # Assign requests to employees
    assignments = {}
    total_assignments = 0

    for i, request in enumerate(requests_list):
        assigned = False

        for i, employee in enumerate(employees):
            time_to_reach = get_duration(request['lat'], request['long'], employee['lat'], employee['long'])
            print(time_to_reach)
            available_for_assignment, reach_time = check_request_within_receiver(request_start=request['valid_from'], 
                                                                                 request_end=request['valid_to'], 
                                                                                 receiver_start=request['valid_from'], 
                                                                                 receiver_end=request['valid_to'], 
                                                                                 time_to_reach=time_to_reach)
            print(available_for_assignment, reach_time)
            gender_validation = check_gender_requirement(employee_gender=employee['gender'], request_target=request['gender_target'])

            if available_for_assignment and gender_validation:
                if employee['name'] not in assignments:
                    assignments[employee['name']] = []

                assignments[employee['name']].append((request['name'], reach_time.strftime("%H:%M:%S")))
                employees[i]['valid_from'] = (reach_time + timedelta(minutes=float(request['visit_duration']))).strftime("%H:%M:%S")
                assigned = True
                total_assignments += 1
                break
        if not assigned:
            logger.warning(f"No available employees for request: {request['name']}")

    results = []
    # Output the assignments with the order of visits
    for employee, assigned_requests in assignments.items():
        logger.debug(f"employee: {employee}")
        
        assignment_item = {"name": employee, "visits": []}

        for idx, (request, reach_time) in enumerate(assigned_requests, 1):
            # total_time += visit_time
            assignment_item["visits"].append({"request_name": request, "reach_time": reach_time})
            logger.debug(f"Visit {idx}: Request {request} handled by {str(employee)} and reached at {str(reach_time)}")

        # logger.debug(f"Total visit time: {total_time} minutes\n")
        # assignment_item["Total visit time"] = total_time

        results.append(assignment_item)
    
    return {
        "request_id": request_id,
        "request_time": request_time,
        "employees_assignments": results, 
        "total_assignments": total_assignments
    }

