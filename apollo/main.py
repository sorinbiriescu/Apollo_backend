import config

######################################
# SETTINGS
######################################

# site_settings = config.SettingsPROD()
# site_settings = config.SettingsDebugLocal()
site_settings = config.SettingsDebugDocker()
# site_settings = config.SettingsTesting()

######################################

import asyncio
import functools
import logging
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta
from logging.handlers import RotatingFileHandler
from typing import Optional, List

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.logger import logger as fastapi_logger
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm

from apollo import auth, crud
from apollo.pipeline import data_workflows
from apollo import schemas

######################################
# PYINSTRUMENT
######################################


# from pyinstrument import Profiler
# profiler = Profiler()

# profiler.start()
# result = models.get_ticket_flow_score(inter_type)
# profiler.stop()
# print(profiler.output_text(unicode=True, color=True))

######################################

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)


######################################
# LOGGER
######################################

formatter = logging.Formatter(
    "[%(asctime)s.%(msecs)03d] %(levelname)s [%(thread)d] [%(process)d] - %(message)s", "%Y-%m-%d %H:%M:%S")
handler = RotatingFileHandler('./apollo-log.log', backupCount=0)
logging.getLogger().setLevel(logging.NOTSET)
fastapi_logger.addHandler(handler)
handler.setFormatter(formatter)


######################################
# PARALLEL PROCESSING
######################################

executor = ProcessPoolExecutor()

async def run_task(func, *param):
    
    loop = asyncio.get_event_loop()

    print("process started")
    return await loop.run_in_executor(executor, functools.partial(func, *param))


######################################
# MAIN
######################################

fastapi_logger.info('****************** Starting Server *****************')

@app.get("/", status_code=200)
async def main_route(current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    return True

######################################
# USER MANAGEMENT AND AUTHENTIFICATION
######################################


@app.post("/api/token")
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = auth.authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(
        minutes=site_settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = auth.create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )

    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/api/register", status_code=status.HTTP_201_CREATED)
def register(user: schemas.UserModel):
    existing_user = auth.get_user(user.username)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User already exists",
            headers={"WWW-Authenticate": "Bearer"},
        )

    new_user_status = crud.register_new_user(user)

    if new_user_status:
        return True


@app.get("/api/users/me", status_code=200)
async def read_users_me(current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    return current_user.username


######################################
# REQUEST CLASSIFICATION
######################################

@app.get("/api/request-classification")
async def get_request_classification():
    return

@app.patch("/api/request-classification", status_code=200)
async def proxi_table_update(params: schemas.RequestClassificationChangeModel,
                            current_user: schemas.UserModel = Depends(auth.get_current_active_user),):
    update_result = crud.update_intervention_type(params.AP_SD_REQUEST_ID, params.Type)
    return update_result

######################################
# DATACALENDAR
######################################

@app.get("/api/employee_mouvement_calendar")
async def employee_mouvement_calendar(current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_employee_mouvement_calendar)
    return result

@app.get("/api/rdv_calendar")
async def rdv_calendar(current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_rdv_calendar)
    return result

######################################
# SUSPENDED MAIL NOT RECONTACTED
######################################

@app.get("/api/suspended_mail_not_recontacted_table", response_model= schemas.SuspendedMailNotRecontactedResponseModel)
async def suspended_mail_not_recontacted_table(tech_filter: Optional[str] = None, current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_suspended_mail_not_recontacted_table, tech_filter)
    return result


@app.get("/api/suspended_mail_not_recontacted_datatable", response_model= List[schemas.DatatableItemResponseModel])
async def suspended_mail_not_recontacted_datatable(tech_filter: Optional[str] = None,
            current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_suspended_mail_not_recontacted_datatable, tech_filter)
    return result


@app.get("/api/suspended_mail_not_recontacted_indicator", response_model= int)
async def suspended_mail_not_recontacted_indicator(tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_suspended_mail_not_recontacted_indicator, tech_filter)
    return result


######################################
# UNDER OBSERVATION TICKETS
######################################

@app.get("/api/under_observation_tickets_datatable", response_model= List[schemas.DatatableItemResponseModel])
async def under_observation_tickets_datatable(tech_filter: Optional[str] = None, current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_tickets_under_observation_datatable, tech_filter)
    return result


@app.get("/api/under_observation_tickets_indicator", response_model= int)
async def under_observation_tickets_indicator(tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_tickets_under_observation_indicator, tech_filter)
    return result


######################################
# SOFTWARE INSTALLATION
######################################

@app.get("/api/software_installation_datatable", response_model= List[schemas.DatatableItemResponseModel])
async def software_installation_datatable(tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_software_installation_datatable, tech_filter)
    return result


@app.get("/api/software_installation_indicator", response_model= int)
async def software_installation_indicator(tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_software_installation_indicator, tech_filter)
    return result


######################################
# NEW ARRIVALS
######################################

@app.get("/api/new_arrivals_datatable", response_model= List[schemas.DatatableItemResponseModel])
async def new_arrivals_datatable(tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_new_arrivals_datatable, tech_filter)
    return result


@app.get("/api/new_arrivals_datatable_indicator", response_model= int)
async def new_arrivals_datatable_indicator(tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_new_arrivals_indicator, tech_filter)
    return result


######################################
# SUSPENDED GREATER THAN X TIME
######################################

@app.get("/api/suspended_gt_x_datatable", response_model= List[schemas.DatatableItemResponseModel])
async def suspended_mail_not_recontacted_datatable(inter_type: str = None, tech_filter: Optional[str] = None, current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_suspended_gt_x_datatable, inter_type, tech_filter)
    return result


@app.get("/api/suspended_gt_x_indicator", response_model= int)
async def suspended_mail_not_recontacted_indicator(inter_type: str = None,
        tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_suspended_gt_x_indicator, inter_type, tech_filter)
    return result


######################################
# CONTACTED X NUMBER OF TIMES
######################################

@app.get("/api/contacted_x_times_datatable", response_model= List[schemas.DatatableItemResponseModel])
async def contacted_x_times_datatable(tech_filter: Optional[str] = None, current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_contacted_x_times_datatable, tech_filter)
    return result


@app.get("/api/contacted_x_times_indicator", response_model= int)
async def contacted_x_times_indicator(tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_contacted_x_times_indicator, tech_filter)
    return result


######################################
# NOT SUSPENDED TICKETS
######################################

@app.get("/api/not_suspended_incidents_datatable", response_model= List[schemas.DatatableItemResponseModel])
async def not_suspended_incidents_datatable(tech_filter: Optional[str] = None, current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_not_suspended_incidents_datatable, tech_filter)
    return result


@app.get("/api/not_suspended_incidents_indicator",response_model= int)
async def not_suspended_incidents_indicator(tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_not_suspended_incidents_indicator, tech_filter)
    return result


######################################
# INDUSTRIAL RELATED TICKETS
######################################

@app.get("/api/industrial_tickets_datatable", response_model= List[schemas.DatatableItemResponseModel])
async def industrial_tickets_datatable(tech_filter: Optional[str] = None, current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_industrial_tickets_datatable, tech_filter)
    return result


@app.get("/api/industrial_tickets_total_indicator", response_model= int)
async def industrial_tickets_total_indicator(tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_industrial_tickets_indicator, tech_filter)
    return result


######################################
# SECURITY RELATED REQUESTS
######################################

@app.get("/api/security_tickets_datatable", response_model= List[schemas.DatatableItemResponseModel])
async def security_tickets_datatable(tech_filter: Optional[str] = None, current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_security_tickets_datatable, tech_filter)
    return result


@app.get("/api/security_tickets_total_indicator", response_model= int)
async def security_tickets_total_indicator(tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_security_tickets_indicator, tech_filter)
    return result


######################################
# VIP RELATED REQUESTS
######################################

@app.get("/api/vip_tickets_datatable", response_model= List[schemas.DatatableItemResponseModel])
async def vip_tickets_datatable(tech_filter: Optional[str] = None, current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_vip_tickets_datatable, tech_filter)
    return result


@app.get("/api/vip_tickets_total_indicator", response_model= int)
async def vip_tickets_total_indicator(tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_vip_tickets_total_indicator, tech_filter)
    return result


######################################
# TICKET FLOW
######################################

@app.get("/api/ticket_flow_datatable", response_model= List[schemas.DatatableItemResponseModel])
async def ticket_flow_datatable(inter_type: Optional[str] = None,
        tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_ticket_flow_datatable, inter_type, tech_filter)
    return result


@app.get("/api/ticket_flow_indicator", response_model= int)
async def ticket_flow_total_indicator(inter_type: Optional[str] = None,
        tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_ticket_flow_indicator, inter_type, tech_filter)
    return result


@app.get("/api/ticket_flow_score", response_model= int)
async def ticket_flow_total_score(inter_type: Optional[str] = None,
        tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_ticket_flow_score, inter_type, tech_filter)
    return result


@app.get("/api/tickets_by_expiration_table", response_model= schemas.TicketsByExpirationResponseModel)
async def tickets_by_expiration_table(inter_type: Optional[str] = None,
        tech_filter: Optional[str] = None,
        current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_tickets_by_expiration_table, inter_type, tech_filter)
    return result

@app.get("/api/technician_total_tickets_table", response_model= schemas.TotalTicketsByTechResponseModel)
async def technician_total_tickets_table(current_user: schemas.UserModel = Depends(auth.get_current_active_user)):
    result = await run_task(data_workflows.flow_get_technician_tickets_table)
    return result
