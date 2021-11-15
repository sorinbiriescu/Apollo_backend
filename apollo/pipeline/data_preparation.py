import re
import unicodedata
from typing import Dict, List, Union

import numpy as np
import pandas as pd

# pd.set_option('mode.chained_assignment', "raise")

def normalize_names(name):
    return ''.join([
        c for c in unicodedata.normalize('NFD', name)
        if unicodedata.category(c) != 'Mn'
    ])


def clean_ticket_data(df: pd.DataFrame) -> pd.DataFrame:
    '''Cleans the request data and renames the columns received from SPOT

    Args:
        df (dataframe): the data in pandas df format
        date_format (str): if the date format is different than "%Y-%m-%d %H:%M:%S.%f"

    Returns:
        df (dataframe): the cleaned data

        Fields in the returned df (note, they have the same name as the SPOT ones but with
        the prefix AP_SD_ in order to avoid accidental SPOT DB changes and to signal inside the
        code from there the data is).
            
    '''
    assert isinstance(df, pd.DataFrame)

    df.replace("N/A", np.nan, inplace=True)
    df.replace(".*<NA>.*", np.nan, regex=True, inplace=True)

    columns = {"REQUEST_ID": "AP_SD_REQUEST_ID",
        "PARENT_REQUEST_ID": "AP_SD_PARENT_REQUEST_ID",
        "RFC_NUMBER": "AP_SD_RFC_NUMBER",
        "CREATION_DATE_UT": "AP_SD_CREATION_DATE_UT",
        "SUBMIT_DATE_UT": "AP_SD_SUBMIT_DATE_UT",
        "END_DATE_UT": "AP_SD_END_DATE_UT",
        "MAX_RESOLUTION_DATE_UT": "AP_SD_MAX_RESOLUTION_DATE",
        "LAST_UPDATE": "LAST_UPDATE",
        "SUBMITTED_BY": "SUBMITTED_BY",
        "SUBMITTED_BY_LAST_NAME": "SUBMITTED_BY_LAST_NAME",
        "REQUESTOR_ID": "REQUESTOR_ID",
        "REQUESTOR_LAST_NAME": "REQUESTOR_LAST_NAME",
        "REQUESTOR_LOCATION_RH": "AP_SD_REQUESTOR_LOCATION_RH",
        "LOCATION_ID": "AP_SD_LOCATION_ID",
        "REQUESTOR_PHONE": "AP_SD_REQUESTOR_PHONE",
        "RECIPIENT_ID": "AP_SD_RECIPIENT_ID",
        "RECIPIENT_LAST_NAME": "AP_SD_RECIPIENT_LAST_NAME",
        "RECIPIENT_LOCATION_RH": "AP_SD_RECIPIENT_LOCATION_RH",
        "SD_CATALOG_ID": "AP_SD_CATALOG_ID",
        "CATALOG_NAME": "AP_SD_CATALOG_NAME",
        "STATUS_ID": "AP_SD_STATUS_ID",
        "STATUS_FR": "AP_SD_STATUS_FR",
        "COMMENT": "AP_SD_COMMENT",
        "DESCRIPTION": "AP_SD_DESCRIPTION",
        "URGENCY_ID": "AP_SD_URGENCY_ID",
        "URGENCY_FR": "AP_SD_URGENCY",
        "CI_ID": "AP_SD_CI_ID",
        "CI_NAME": "AP_SD_CI_NAME",
        "OWNER_ID": "AP_SD_OWNER_ID",
        "LAST_NAME": "AP_SD_LAST_NAME",
        "OWNING_GROUP_ID": "AP_SD_OWNING_GROUP_ID",
        "GROUP_FR": "AP_SD_GROUP_FR",
        "DEPARTMENT_ID": "AP_SD_DEPARTMENT_ID",
        "REQUESTOR_FEEDBACK": "AP_SD_REQUESTOR_FEEDBACK",
        "ASSET_ID": "AP_SD_ASSET_ID",
        "ASSET_TAG": "AP_SD_MATERIAL_NAME",
        "FIRST_CALL_RESOLUTION": "AP_SD_FIRST_CALL_RESOLUTION",
        "SEVERITY_ID": "AP_SD_SEVERITY_ID",
        "TIME_USED_TO_SOLVE_REQUEST": "AP_SD_TIME_USED_TO_SOLVE_REQUEST",
        "REQUEST_ORIGIN_ID": "AP_SD_REQUEST_ORIGIN_ID",
        "DELAY": "AP_SD_DELAY",
        "SYSTEM_ID": "AP_SD_SYSTEM_ID",
        "LAST_GROUP_ID": "AP_SD_LAST_GROUP_ID",
        "LAST_DONE_BY_ID": "AP_SD_LAST_DONE_BY_ID",
        "SLA_ID": "AP_SD_SLA_ID",
        "IMPACT_ID": "AP_SD_IMPACT_ID",
        "E_INFRA_COMMENT": "AP_SD_E_INFRA_COMMENT",
        "IS_MAJOR_INCIDENT": "AP_SD_IS_MAJOR_INCIDENT",
        "E_TYPE_SUPPORT": "AP_SD_SUPPORT_TYPE"
        }

    df.rename(columns = columns, inplace = True)

    # df = df.loc[:("AP_SD_RFC_NUMBER", "AP_SD_STATUS_FR","AP_SD_PARENT_REQUEST_ID", "AP_SD_MAX_RESOLUTION_DATE", "AP_SD_RECIPIENT_LAST_NAME", "AP_SD_CATALOG_NAME", "AP_SD_URGENCY", "AP_SD_SUPPORT_TYPE", "location",
    #             "AP_SD_MATERIAL_NAME", "AP_SD_CI_NAME", "AP_SD_COMMENT")]
    df.loc[:,"AP_SD_RFC_NUMBER"] = df["AP_SD_RFC_NUMBER"].astype(str)
    df.loc[:,"AP_SD_STATUS_FR"] = df["AP_SD_STATUS_FR"].astype("category")
    df.loc[:,"AP_SD_PARENT_REQUEST_ID"] = df["AP_SD_PARENT_REQUEST_ID"].astype("category")

    df.loc[:,"AP_SD_CREATION_DATE_UT"] = pd.to_datetime(df["AP_SD_CREATION_DATE_UT"],
                        format = "%Y-%m-%d %H:%M:%S.%f", 
                        errors = "coerce",
                        utc = True)
    df.loc[:,"AP_SD_CREATION_DATE_UT"] = df["AP_SD_CREATION_DATE_UT"].dt.tz_convert('Europe/Paris')

    df.loc[:,"AP_SD_SUBMIT_DATE_UT"] = pd.to_datetime(df["AP_SD_SUBMIT_DATE_UT"],
                        format = "%Y-%m-%d %H:%M:%S.%f", 
                        errors = "coerce",
                        utc = True)
    df.loc[:,"AP_SD_SUBMIT_DATE_UT"] = df["AP_SD_SUBMIT_DATE_UT"].dt.tz_convert('Europe/Paris')

    df.loc[:,"AP_SD_END_DATE_UT"] = pd.to_datetime(df["AP_SD_END_DATE_UT"],
                        format = "%Y-%m-%d %H:%M:%S.%f",
                        errors = "coerce",
                        utc = True)
    df.loc[:,"AP_SD_END_DATE_UT"] = df["AP_SD_END_DATE_UT"].dt.tz_convert('Europe/Paris')

    df.loc[:,"AP_SD_MAX_RESOLUTION_DATE"] = pd.to_datetime(df["AP_SD_MAX_RESOLUTION_DATE"],
                        format = "%Y-%m-%d %H:%M:%S.%f",
                        errors = "coerce",
                        utc = True)
    df.loc[:,"AP_SD_MAX_RESOLUTION_DATE"] = df["AP_SD_MAX_RESOLUTION_DATE"].dt.tz_convert('Europe/Paris')

    df.loc[:,"AP_SD_RECIPIENT_LAST_NAME"] = df["AP_SD_RECIPIENT_LAST_NAME"].astype(str)
    df.loc[:,"AP_SD_RECIPIENT_LAST_NAME"].fillna("N/A", inplace=True)
    df.loc[:,"AP_SD_RECIPIENT_LAST_NAME"] = df["AP_SD_RECIPIENT_LAST_NAME"].map(normalize_names)
    df.loc[:,"AP_SD_CATALOG_NAME"] = df["AP_SD_CATALOG_NAME"].astype("category")
    df.loc[:,"AP_SD_URGENCY"] = df["AP_SD_URGENCY"].astype("category")
    if "4-Normal" not in df["AP_SD_URGENCY"].cat.categories:
        df.loc[:,"AP_SD_URGENCY"] = df["AP_SD_URGENCY"].cat.add_categories("4-Normal")
    df.loc[:,"AP_SD_URGENCY"].fillna("4-Normal", inplace=True)
    df.loc[:,"AP_SD_SUPPORT_TYPE"] = df["AP_SD_SUPPORT_TYPE"].astype("category")
    df.loc[:,"AP_SD_RECIPIENT_LOCATION_RH"].fillna("Pas de location", inplace = True)
    df.loc[:,"AP_SD_RECIPIENT_LOCATION_RH"] = df["AP_SD_RECIPIENT_LOCATION_RH"].astype("category")
    df.loc[:,"AP_SD_MATERIAL_NAME"] = df["AP_SD_MATERIAL_NAME"].astype(str)
    df.loc[:,"AP_SD_CI_NAME"] = df["AP_SD_CI_NAME"].astype("category")
    df.loc[:,"AP_SD_COMMENT"] = df["AP_SD_COMMENT"].astype(str)
    df.loc[:,"AP_SD_COMMENT"] = df["AP_SD_COMMENT"].str.replace("<[^<]+?>","")
    df.loc[:,"AP_SD_COMMENT"] = df["AP_SD_COMMENT"].str.replace("bonjour.?","", flags = re.IGNORECASE)

    # TODO add check to see if cat column is in df column list
    categorical_cols = ["AP_SD_STATUS_FR", "AP_SD_PARENT_REQUEST_ID", "AP_SD_CATALOG_NAME", "AP_SD_URGENCY", "AP_SD_SUPPORT_TYPE"]
    for x in categorical_cols:
        if "-" not in df[x].cat.categories:
            df.loc[:,x] = df[x].cat.add_categories("-")

    # if "Pas de location" not in df["AP_SD_RECIPIENT_LOCATION_RH"]:
    #     df["AP_SD_RECIPIENT_LOCATION_RH"] = df["AP_SD_RECIPIENT_LOCATION_RH"].cat.add_categories("Pas de location")

    assert isinstance(df, pd.DataFrame)
    return df


def clean_ticket_tasks_data(df, date_format = None):
    '''Cleans the request data actions (taches) and renames the columns received from SPOT

    Args:
        df (dataframe): the data in pandas df format
        date_format (str): if the date format is different than "%Y-%m-%d %H:%M:%S.%f"

    Returns:
        df (dataframe): the cleaned data

        Fields in the returned df (note, they have the same name as the SPOT ones but with
        the prefix AP_AM_ in order to avoid accidental SPOT DB changes and to signal inside the
        code from there the data is).
            
    '''
    assert isinstance(df, pd.DataFrame)

    columns = {"ACTION_ID": "AP_AM_ACTION_ID",
        "REQUEST_ID": "AP_AM_REQUEST_ID",
        "RFC_NUMBER": "AP_AM_RFC_NUMBER",
        "ACTION_NUMBER": "AP_AM_ACTION_NUMBER",
        "ASSET_ID": "AP_AM_ASSET_ID",
        "PARENT_ACTION_ID": "AP_AM_PARENT_ACTION_ID",
        "VALIDATOR_ID": "AP_AM_VALIDATOR_ID",
        "ACTION_LABEL_FR": "AP_AM_OPERATION_TYPE_NAME",
        "ACTION_TYPE_ID": "AP_AM_ACTION_TYPE_ID",
        "ACTION_TYPE": "AP_AM_ACTION_TYPE",
        "START_DATE_UT": "AP_AM_START_DATE",
        "END_DATE_UT": "AP_AM_END_DATE_UT",
        "EXPECTED_START_DATE_UT": "AP_AM_EXPECTED_START_DATE_UT",
        "CREATION_DATE_UT": "AP_AM_CREATION_DATE_UT",
        "EXPECTED_END_DATE_UT": "AP_AM_EXPECTED_END_DATE_UT",
        "LAST_UPDATE": "AP_AM_LAST_UPDATE",
        "DESCRIPTION": "AP_AM_DESCRIPTION",
        "PROCESS_STEP_ID": "AP_AM_PROCESS_STEP_ID",
        "LOCATION_ID": "AP_AM_LOCATION_ID",
        "DONE_BY_ID": "AP_AM_DONE_BY_ID",
        "DONE_BY_NAME": "AP_AM_DONE_BY_OPERATOR_NAME",
        "GROUP_ID": "AP_AM_GROUP_ID",
        "GROUP_FR": "AP_AM_GROUP_FR",
        "CONTACT_ID": "AP_AM_CONTACT_ID",
        "ELAPSED_TIME": "AP_AM_ELAPSED_TIME",
        "STATUS_ID_ON_CREATE": "AP_AM_STATUS_ID_ON_CREATE",
        "STATUS_ID_ON_TERMINATE": "AP_AM_STATUS_ID_ON_TERMINATE",
        "TIME_USED_TO_COMPLETE_ACTION": "AP_AM_TIME_USED_TO_COMPLETE_ACTION",
        "ORIGIN_ACTION_ID": "AP_AM_ORIGIN_ACTION_ID",
        "WORKFLOW_VALUE": "AP_AM_WORKFLOW_VALUE",
        "WORKFLOW_ID": "AP_AM_WORKFLOW_ID",
        "EXIT_VALUE": "AP_AM_EXIT_VALUE",
        "PREVIOUS_SIBLING_ID": "AP_AM_PREVIOUS_SIBLING_ID",
        "HISTORY_ID": "AP_AM_HISTORY_ID"
    }

    df.rename(columns = columns, inplace = True)

    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    df.dropna(subset=["AP_AM_START_DATE", "AP_AM_DONE_BY_OPERATOR_NAME", "AP_AM_OPERATION_TYPE_NAME"], how="all", inplace=True)
    df.loc[:,"AP_AM_RFC_NUMBER"] = df["AP_AM_RFC_NUMBER"].astype(str)
    df.loc[:,"AP_AM_ACTION_TYPE_ID"] = df["AP_AM_ACTION_TYPE_ID"].astype("category")
    df.loc[df["AP_AM_DONE_BY_OPERATOR_NAME"].isnull(),"AP_AM_DONE_BY_OPERATOR_NAME"] =  df.loc[df["AP_AM_DONE_BY_OPERATOR_NAME"].isnull(),"AP_AM_GROUP_FR"]
    df.loc[:,"AP_AM_DONE_BY_OPERATOR_NAME"] = df["AP_AM_DONE_BY_OPERATOR_NAME"].astype("category")
    
    df.loc[:,"AP_AM_START_DATE"] = pd.to_datetime(df["AP_AM_START_DATE"],
                        format = (lambda x: x if x else "%Y-%m-%d %H:%M:%S.%f")(date_format),
                        errors = "coerce",
                        utc = True)
    df.loc[:,"AP_AM_START_DATE"] = df["AP_AM_START_DATE"].dt.tz_convert('Europe/Paris')

    df.loc[:,"AP_AM_END_DATE_UT"] = pd.to_datetime(df["AP_AM_END_DATE_UT"],
                        format = (lambda x: x if x else "%Y-%m-%d %H:%M:%S.%f")(date_format),
                        errors = "coerce",
                        utc = True)
    df.loc[:,"AP_AM_END_DATE_UT"] = df["AP_AM_END_DATE_UT"].dt.tz_convert('Europe/Paris')

    df.loc[:,"AP_AM_CREATION_DATE_UT"] = pd.to_datetime(df["AP_AM_CREATION_DATE_UT"],
                        format = (lambda x: x if x else "%Y-%m-%d %H:%M:%S.%f")(date_format),
                        errors = "coerce",
                        utc = True)
    df.loc[:,"AP_AM_CREATION_DATE_UT"] = df["AP_AM_CREATION_DATE_UT"].dt.tz_convert('Europe/Paris')

    df.loc[:,"AP_AM_LAST_UPDATE"] = pd.to_datetime(df["AP_AM_LAST_UPDATE"],
                        format = (lambda x: x if x else "%Y-%m-%d %H:%M:%S.%f")(date_format),
                        errors = "coerce",
                        utc = True)
    df.loc[:,"AP_AM_LAST_UPDATE"] = df["AP_AM_LAST_UPDATE"].dt.tz_convert('Europe/Paris')

    df.loc[:,"AP_AM_OPERATION_TYPE_NAME"] = df["AP_AM_OPERATION_TYPE_NAME"].astype("category")

    categorical_cols = ["AP_AM_OPERATION_TYPE_NAME"]
    for x in categorical_cols:
        if "-" not in df[x].cat.categories:
            df.loc[:,x] = df[x].cat.add_categories("-")

    categorical_cols = ["AP_AM_DONE_BY_OPERATOR_NAME"]

    # for x in categorical_cols:
    #     if "Hotline" not in df[x].cat.categories:
    #         df[x] = df[x].cat.add_categories("Hotline")

    assert isinstance(df, pd.DataFrame)
    return df

def correct_spot_bugs_request(df):
    assert isinstance(df, pd.DataFrame)

    # files 188214-I19120809 and 185394-I19110161 not properly closed
    df = df.loc[~(df["AP_SD_REQUEST_ID"].isin([188214, 185394]))]
    return df


def correct_spot_bugs_request_operations(df):
    assert isinstance(df, pd.DataFrame)

    # files 188214-I19120809 and 185394-I19110161 not properly closed
    df = df.loc[~(df["AP_AM_REQUEST_ID"].isin([188214, 185394]))]
    return df   

# @task
# def pf_merge_results(df_list):
#     def merge_df(df1, df2):
#         return pd.merge(df1, df2, how='inner')
        
#     df = reduce(merge_df, df_list)
#     return df


def format_df_before_dispatch(df) -> List[Dict[str, Union[str, int]]]:
    assert isinstance(df, pd.DataFrame)

    df = df.loc[:,("AP_SD_RFC_NUMBER",
        "AP_SD_STATUS_FR",
        "AP_SD_RECIPIENT_LAST_NAME",
        "AP_SD_RECIPIENT_LOCATION_RH",
        "AP_SD_URGENCY",
        "AP_SD_CI_NAME",
        "AP_SD_COMMENT",
        "C_RDV_DATE",
        "C_RDV_STATE",
        "AP_AM_DONE_BY_OPERATOR_NAME",
        "C_POINTS",
        "C_POINTS_JUSTIFICATION",
        "AP_INTERVENTION_TYPE",
        "AP_TYPE_FR",
        "C_TICKET_TYPE",
        "C_TICKET_TYPE_STRING_FR",
        "AP_SD_REQUEST_ID")]

    location_translator = {
        "CNR DIR REGION BELLEY": "DHR Belley",
        "CNR AMGT BELLEY-BREG CORD": "DHR Bregnier-Cordon",
        "CNR AMGT GENISSIAT": "DHR Genissiat",
        "CNR AMGT SAULT BRENAZ-LOY": "DHR Sault Brenaz",
        "CNR AMGT SEYSSEL-CHAUTAGN": "DHR Seyssel",
        "CNR SIEGE SOCIAL": "DM Siege",
        "CNR DELEGATION DE PARIS": "DM Paris",
        "CNR LABORATOIRE GERLAND": "DM CACOH",
        "CNR PORT EDOUARD HERRIOT": "DM PLEH",
        "CNR USINE PIERRE BENITE": "DM Pierre Benite",
        "CNR DIR REGION VIENNE": "DRS Vienne",
        "CNR JEAN BART": "DRS Jean Bart",
        "Bureau MAINTENANCE": "DRS Jean Bart",
        "CNR USINE DE GERVANS": "DRS Gervans",
        "CNR USINE DE SABLONS": "DRS Sablons",
        "CNR USINE DE VAUGRIS": "DRS Vaugris",
        "CNR DIR REGION VALENCE": "DRI Valence",
        "CNR USINE BOURG LES VALENCE": "DRI Bourg-les-Valences",
        "CNR USINE DE LOGIS NEUF": "DRI Logis-Neuf",
        "CNR USINE DE CHATEAUNEUF": "DRI CH9",
        "CNR USINE DE BEAUCHASTEL": "DRI Beauchastel",
        "CNR DIR REGION AVIGNON": "DRM Avignon",
        "CNR USINE D AVIGNON": "DRM Usine Avignon",
        "CNR USINE DE BEAUCAIRE": "DRM Beaucaire",
        "CNR USINE DE BOLLENE": "DRM Bollene",
        "CNR USINE DE CADEROUSSE": "DRM Caderousse",
        "CNR USINE DE BARCARIN": "DRM Barcarin"
    }

    if "AP_SD_RECIPIENT_LOCATION_RH" in df.columns:
        df.replace(location_translator, inplace = True)
        df.loc[df["AP_SD_RECIPIENT_LOCATION_RH"].isnull(), ("AP_SD_RECIPIENT_LOCATION_RH")] = "Pas de location"

    categorical_cols = ["AP_SD_STATUS_FR", "AP_SD_PARENT_REQUEST_ID", "AP_SD_CATALOG_NAME",
        "AP_SD_RECIPIENT_LOCATION_RH", "AP_SD_CI_NAME", "AP_SD_URGENCY", "AP_SD_SUPPORT_TYPE"]
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # df.loc[:,"C_TICKET_AGE"] = (df["C_TICKET_AGE"] / pd.Timedelta("1min")).astype(int)
    # df.loc[:,"C_LAST_ACTION_DATE"] = (df["C_LAST_ACTION_DATE"] / pd.Timedelta("1min")).astype(int)
    df["C_RDV_DATE"] = (df["C_RDV_DATE"] - pd.Timestamp("1970-01-01").tz_localize('Europe/Paris')) // pd.Timedelta('1ms')
    fill_values = {
            "C_RDV_DATE" : ""
        }
    df["C_RDV_DATE"] = df["C_RDV_DATE"].fillna(value = fill_values)
    df["C_RDV_DATE"] = df["C_RDV_DATE"].astype(str)

    datetime_cols = ["AP_SD_MAX_RESOLUTION_DATE", "AP_SD_START_DATE","C_LAST_ACTION_DATE", "age"]
    for col in datetime_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    df.loc[:,"C_POINTS"] = df["C_POINTS"].astype(str)
    df.loc[:,"AP_TYPE_FR"] = df["AP_TYPE_FR"].astype(str)

    df.rename(columns={
        "AP_SD_RFC_NUMBER": "SPOT",
        "AP_SD_STATUS_FR": "Statut",
        "AP_SD_RECIPIENT_LAST_NAME": "Beneficiaire",
        "AP_SD_RECIPIENT_LOCATION_RH": "Location",
        "AP_SD_URGENCY": "Priorite",
        "AP_SD_CI_NAME": "CI",
        "AP_SD_COMMENT": "Description",
        "C_RDV_DATE": "C_RDV_DATE",
        "C_RDV_STATE": "C_RDV_STATE",
        "AP_SD_DONE_BY_OPERATOR_NAME": "Operateur",
        "C_POINTS": "Score",
        "AP_TYPE_FR": "Inter_Type"
    },
    inplace=True)

    result: List[Dict[str, Union[str, int]]] = df.to_dict(orient="records")
    return result


def get_suspended_mail_not_recontacted_table(df) -> Dict[str, List[Dict[str, Union[str, int]]]]:
    assert isinstance(df, pd.DataFrame)
    df.sort_values(by = "AP_SD_MAX_RESOLUTION_DATE", inplace = True)
    df = df.loc[:,("AP_SD_RFC_NUMBER", "AP_TYPE_FR", "AP_SD_RECIPIENT_LOCATION_RH", "AP_AM_DONE_BY_OPERATOR_NAME")]
    df = df.head(25)

    location_translator = {
        "CNR DIR REGION BELLEY": "DHR Belley",
        "CNR AMGT BELLEY-BREG CORD": "DHR Bregnier-Cordon",
        "CNR AMGT GENISSIAT": "DHR Genissiat",
        "CNR AMGT SAULT BRENAZ-LOY": "DHR Sault Brenaz",
        "CNR AMGT SEYSSEL-CHAUTAGN": "DHR Seyssel",
        "CNR SIEGE SOCIAL": "DM Siege",
        "CNR DELEGATION DE PARIS": "DM Paris",
        "CNR LABORATOIRE GERLAND": "DM CACOH",
        "CNR PORT EDOUARD HERRIOT": "DM PLEH",
        "CNR USINE PIERRE BENITE": "DM Pierre Benite",
        "CNR DIR REGION VIENNE": "DRS Vienne",
        "CNR JEAN BART": "DRS Jean Bart",
        "Bureau MAINTENANCE": "DRS Jean Bart",
        "CNR USINE DE GERVANS": "DRS Gervans",
        "CNR USINE DE SABLONS": "DRS Sablons",
        "CNR USINE DE VAUGRIS": "DRS Vaugris",
        "CNR DIR REGION VALENCE": "DRI Valence",
        "CNR USINE BOURG LES VALENCE": "DRI Bourg-les-Valences",
        "CNR USINE DE LOGIS NEUF": "DRI Logis-Neuf",
        "CNR USINE DE CHATEAUNEUF": "DRI CH9",
        "CNR USINE DE BEAUCHASTEL": "DRI Beauchastel",
        "CNR DIR REGION AVIGNON": "DRM Avignon",
        "CNR USINE D AVIGNON": "DRM Usine Avignon",
        "CNR USINE DE BEAUCAIRE": "DRM Beaucaire",
        "CNR USINE DE BOLLENE": "DRM Bollene",
        "CNR USINE DE CADEROUSSE": "DRM Caderousse",
        "CNR USINE DE BARCARIN": "DRM Barcarin"
    }

    if "AP_SD_RECIPIENT_LOCATION_RH" in df.columns:
        df.replace(location_translator, inplace = True)
        df.loc[df["AP_SD_RECIPIENT_LOCATION_RH"].isnull(), ("AP_SD_RECIPIENT_LOCATION_RH")] = "Pas de location"

    suspended_mail_not_recontacted_tickets: Dict[str, List[Dict[str, Union[str, int]]]] = {
        "columns" : [{
            "col_order": 0,
            "name": "Ticket",
            "id": "AP_SD_RFC_NUMBER"
        },
        {
            "col_order": 1,
            "name": "Type",
            "id": "AP_TYPE_FR"
        },
        {
            "col_order": 2,
            "name": "Location",
            "id": "AP_SD_RECIPIENT_LOCATION_RH"
        },
        {
            "col_order": 3,
            "name": "Technician",
            "id": "AP_AM_DONE_BY_OPERATOR_NAME"
        }],
        "data": []
    }

    suspended_mail_not_recontacted_tickets["data"] = df.to_dict("records")

    return suspended_mail_not_recontacted_tickets
