import asyncio
from typing import Any, Dict, Union

import pandas as pd
from apollo.main import site_settings
from apollo.database import create_spot_engine
from apollo.pipeline import (data_analysis, data_collection, data_preparation,
                             data_visualisation, feature_engineering)
from pymemcache import serde
from pymemcache.client.base import Client

lock = asyncio.Lock()
event_loop = asyncio.get_event_loop()


###############################
# MEMCACHE CLIENT
###############################

memcached_client = Client(
    (site_settings.MEMCACHE_CLIENT, 11211), serde=serde.pickle_serde)

###############################

######################################
# EMPLOYEE MOUVEMENT CALENDAR
######################################

def flow_get_employee_mouvement_calendar():
    df = data_collection.get_employee_mouvement_data(
        engine=create_spot_engine, memcache=memcached_client)
    df = feature_engineering.get_employee_movement_question_info(df)

    result = data_analysis.get_employee_mouvement(df)

    return result


######################################
# EMPLOYEE MOUVEMENT CALENDAR
######################################

def flow_get_rdv_calendar():
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations)

    df = data_analysis.get_rdv_date_state(df, df_operations)

    result = data_analysis.get_rdv_dates(df)

    return result


######################################
# TICKETS UNDER OBSERVATION
######################################

def flow_get_tickets_under_observation_datatable(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    # lock.release()

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_rdv_date_state(df, df_operations)
    df = data_analysis.calculate_ticket_flow(df, df_operations)

    df = data_analysis.get_under_observation_tickets(df, df_operations)

    result = data_preparation.format_df_before_dispatch(df)

    return result



def flow_get_tickets_under_observation_indicator(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))


    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_under_observation_tickets(df, df_operations)

    result = data_analysis.get_df_len(df)
    return result

######################################


######################################
# SUSPENDED MAIL NOT RECONTACTED
######################################

def flow_get_software_installation_datatable(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_rdv_date_state(df, df_operations)
    df = data_analysis.calculate_ticket_flow(df, df_operations)

    df = data_analysis.get_tickets_software_install(df)

    result = data_preparation.format_df_before_dispatch(df)

    return result


def flow_get_software_installation_indicator(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_tickets_software_install(df)

    result = data_analysis.get_df_len(df)
    return result

######################################


######################################
# SUSPENDED MAIL NOT RECONTACTED
######################################

def flow_get_new_arrivals_datatable(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_rdv_date_state(df, df_operations)
    df = data_analysis.calculate_ticket_flow(df, df_operations)

    df = data_analysis.get_new_arrival_tickets(df)

    result = data_preparation.format_df_before_dispatch(df)

    return result


def flow_get_new_arrivals_indicator(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_new_arrival_tickets(df)

    result = data_analysis.get_df_len(df)
    return result

######################################


######################################
# SUSPENDED MAIL NOT RECONTACTED
######################################

def flow_get_suspended_mail_not_recontacted_table(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_suspended_mail_not_recontacted(df, df_operations)

    result = data_preparation.get_suspended_mail_not_recontacted_table(df)
    return result


def flow_get_suspended_mail_not_recontacted_datatable(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_rdv_date_state(df, df_operations)
    df = data_analysis.calculate_ticket_flow(df, df_operations)

    df = data_analysis.get_suspended_mail_not_recontacted(df, df_operations)

    result = data_preparation.format_df_before_dispatch(df)

    return result


def flow_get_suspended_mail_not_recontacted_indicator(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)

    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_suspended_mail_not_recontacted(df, df_operations)

    result = data_analysis.get_df_len(df)
    return result

######################################


######################################
# SUSPENDED GREATER THAN X TIME
######################################

def flow_get_suspended_gt_x_datatable(inter_type_filter, tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_rdv_date_state(df, df_operations)
    df = data_analysis.calculate_ticket_flow(df, df_operations)

    df = data_analysis.get_suspended_gt_x(df, df_operations, inter_type_filter)

    result = data_preparation.format_df_before_dispatch(df)

    return result


def flow_get_suspended_gt_x_indicator(inter_type_filter, tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_suspended_gt_x(df, df_operations, inter_type_filter)

    result = data_analysis.get_df_len(df)
    return result

######################################


######################################
# CONTACTED X NUMBER OF TIMES
######################################

def flow_get_contacted_x_times_datatable(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_rdv_date_state(df, df_operations)
    df = data_analysis.calculate_ticket_flow(df, df_operations)

    df = data_analysis.get_contacted_x_times(df, df_operations, 3)

    result = data_preparation.format_df_before_dispatch(df)

    return result


def flow_get_contacted_x_times_indicator(tech_filter: str = None) -> int:
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)

    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_contacted_x_times(df, df_operations, 3)
    result: int = data_analysis.get_df_len(df)
    return result

######################################


######################################
# NOT SUSPENDED REQUESTS
######################################

def flow_get_not_suspended_incidents_datatable(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_rdv_date_state(df, df_operations)
    df = data_analysis.calculate_ticket_flow(df, df_operations)

    df = data_analysis.get_not_suspended_incidents(df)

    result = data_preparation.format_df_before_dispatch(df)

    return result


def flow_get_not_suspended_incidents_indicator(tech_filter: str = None) -> int:
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_not_suspended_incidents(df)
    result: int = data_analysis.get_df_len(df)
    return result

######################################


######################################
# INDUSTRIAL RELATED REQUESTS
######################################

def flow_get_industrial_tickets_datatable(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_rdv_date_state(df, df_operations)
    df = data_analysis.calculate_ticket_flow(df, df_operations)

    df = data_analysis.get_industrial_tickets(df)

    result = data_preparation.format_df_before_dispatch(df)

    return result


def flow_get_industrial_tickets_indicator(tech_filter: str = None) -> int:
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_industrial_tickets(df)
    result = data_analysis.get_df_len(df)
    return result

######################################


######################################
# SECURITY RELATED REQUESTS
######################################

def flow_get_security_tickets_datatable(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_rdv_date_state(df, df_operations)
    df = data_analysis.calculate_ticket_flow(df, df_operations)

    df = data_analysis.get_av_security_tickets(df)

    result = data_preparation.format_df_before_dispatch(df)

    return result


def flow_get_security_tickets_indicator(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_av_security_tickets(df)
    result = data_analysis.get_df_len(df)
    return result

######################################


######################################
# VIP RELATED REQUESTS
######################################

def flow_get_vip_tickets_total_indicator(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_vip_tickets(df, vip_list=site_settings.VIP_LIST)
    result = data_analysis.get_df_len(df)
    return result


def flow_get_vip_tickets_datatable(tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_rdv_date_state(df, df_operations)
    df = data_analysis.calculate_ticket_flow(df, df_operations)

    df = data_analysis.get_vip_tickets(df, vip_list=site_settings.VIP_LIST)

    result = data_preparation.format_df_before_dispatch(df)

    return result

######################################


######################################
# GENERAL / HOTLINE / PROXI REQUESTS
######################################

def flow_get_ticket_flow_datatable(inter_type: str = None, tech_filter: str = None):
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.get_rdv_date_state(df, df_operations)
    df = data_analysis.calculate_ticket_flow(df, df_operations)

    df = data_analysis.get_ticket_flow(df, inter_type=inter_type)

    result = data_preparation.format_df_before_dispatch(df)

    return result


def flow_get_ticket_flow_indicator(inter_type=None, tech_filter=None, test_data = None):
    e1: pd.DataFrame
    df_uuid: str

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]

    if test_data:
        e1 = pd.DataFrame(test_data[0])
        df_uuid = "123"

        e2 = pd.DataFrame(test_data[1])
        df_operations_uuid = "123"
    else:
        e1, df_uuid = asyncio.run(data_collection.get_requests(
                engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

        e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations, tech_filter)

    df = data_analysis.calculate_ticket_flow(df, df_operations)

    df = data_analysis.get_ticket_flow(df, inter_type=inter_type)
    result = data_analysis.get_df_len(df)
    return result


def flow_get_ticket_flow_score(inter_type: Union[str, None] = None, tech_filter=None) -> int:
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    t1: pd.DataFrame = data_preparation.clean_ticket_data(e1)
    t2: pd.DataFrame = data_preparation.correct_spot_bugs_request(t1)
    t3: pd.DataFrame = data_preparation.clean_ticket_tasks_data(e2)
    t4: pd.DataFrame = data_preparation.correct_spot_bugs_request_operations(
        t3)

    t5: pd.DataFrame = feature_engineering.get_ticket_type(t2)
    t6: pd.DataFrame = feature_engineering.get_start_date(t5, t4)
    t7: pd.DataFrame = feature_engineering.get_tickets_last_action_date(t6, t4)
    t8: pd.DataFrame = feature_engineering.get_ticket_age(t7)
    t9: pd.DataFrame = feature_engineering.get_ticket_classification(t8)
    t10: pd.DataFrame = feature_engineering.get_ticket_tech_affectation(
        t9, t4, tech_filter)

    l1: pd.DataFrame = data_analysis.calculate_ticket_flow(t10, t4)

    l2: pd.DataFrame = data_analysis.get_ticket_flow(l1, inter_type=inter_type)
    result: int = data_analysis.get_total_score(l2)
    return result


def flow_get_tickets_by_expiration_table(intertype: str = None, tech_filter: str = None) -> Dict[Any, Any]:
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    t1: pd.DataFrame = data_preparation.clean_ticket_data(e1)
    t2: pd.DataFrame = data_preparation.correct_spot_bugs_request(t1)
    t3: pd.DataFrame = data_preparation.clean_ticket_tasks_data(e2)
    t4: pd.DataFrame = data_preparation.correct_spot_bugs_request_operations(
        t3)

    t5: pd.DataFrame = feature_engineering.get_ticket_type(t2)
    t6: pd.DataFrame = feature_engineering.get_ticket_classification(t5)
    t7: pd.DataFrame = feature_engineering.get_ticket_tech_affectation(
        t6, t4, tech_filter)

    r1: Dict[Any, Any] = data_analysis.get_tickets_by_expiration_time_table(t7)

    return r1

def flow_get_technician_tickets_table() -> Dict[Any, Any]:
    e1: pd.DataFrame
    df_uuid: str
    e1, df_uuid = asyncio.run(data_collection.get_requests(
            engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    e2: pd.DataFrame
    df_operations_uuid: Union[str, None]
    e2, df_operations_uuid = asyncio.run(data_collection.get_tasks(
        engine=create_spot_engine, memcached_client=memcached_client, lock= lock))

    df = data_preparation.clean_ticket_data(e1)
    df = data_preparation.correct_spot_bugs_request(df)
    df_operations = data_preparation.clean_ticket_tasks_data(e2)
    df_operations = data_preparation.correct_spot_bugs_request_operations(
        df_operations)

    df = feature_engineering.get_ticket_type(df)
    df = feature_engineering.get_start_date(df, df_operations)
    df = feature_engineering.get_tickets_last_action_date(df, df_operations)
    df = feature_engineering.get_ticket_age(df)
    df = feature_engineering.get_ticket_classification(df)
    df = feature_engineering.get_ticket_tech_affectation(
        df, df_operations)

    df = data_analysis.calculate_ticket_flow(df, df_operations)

    r1: Dict[Any, Any] = data_analysis.get_tickets_score_by_technician_table(df)

    return r1
