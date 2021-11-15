import re
from datetime import timedelta
from typing import Union

import pandas as pd
from apollo.crud import get_requests_intervention_types


def get_ticket_tech_affectation(df: pd.DataFrame, df_operations: pd.DataFrame, tech_filter: str = None) -> pd.DataFrame:
    assert isinstance(df, pd.DataFrame)
    assert isinstance(df_operations, pd.DataFrame)

    df_operations = df_operations.loc[:,("AP_AM_ACTION_ID", "AP_AM_REQUEST_ID", "AP_AM_ACTION_TYPE_ID", "AP_AM_START_DATE", "AP_AM_DONE_BY_OPERATOR_NAME")]
    df_operations = df_operations[df_operations["AP_AM_REQUEST_ID"].isin(df["AP_SD_REQUEST_ID"])]

    # **	20	Traitement Operation
    # **	21	Traitement Transition
    # **	32	Validation Operation
    # **	34	Clôture Service Desk
    # **	37	Clôture Transition
    # **	38	Validation Self Service avec Rating
    # **	50	Traitement Automatique
    # **	51	Affectation à un incident parent
    # **	52	Redirigé vers un niveau inférieur
    # **	54	Installation
    df_operations = df_operations.loc[df_operations["AP_AM_ACTION_TYPE_ID"].isin([20, 21, 32, 34, 37, 38, 50, 52, 54])]
    df_operations = df_operations.sort_values(by = ["AP_AM_START_DATE"], ascending = False, na_position = "first")
    df_operations = df_operations.groupby("AP_AM_REQUEST_ID").first()
    
    assert len(df_operations.index) == len(df.index)

    df = df.merge(df_operations, left_on = "AP_SD_REQUEST_ID", right_index = True, how = "left", suffixes=(False, False))

    if tech_filter:
        df = df.loc[df["AP_AM_DONE_BY_OPERATOR_NAME"].str.contains(tech_filter, regex = True, flags = re.IGNORECASE)]

    assert isinstance(df, pd.DataFrame)
    return df


def get_ticket_type(df):
    assert isinstance(df, pd.DataFrame)

    # 0 - incident
    # 1 - request
    df.loc[:,"C_TICKET_TYPE"] = 0
    df.loc[:,"C_TICKET_TYPE_STRING_FR"] = "Incident"
    df.loc[df["AP_SD_RFC_NUMBER"].str.startswith("D"), "C_TICKET_TYPE"] = 1
    df.loc[df["AP_SD_RFC_NUMBER"].str.startswith("D"), "C_TICKET_TYPE_STRING_FR"] = "Demande"

    assert isinstance(df, pd.DataFrame)
    return df


def get_start_date(df, df_operations):
    """ Returns the df with a creation_date column
    """
    df.loc[:,"C_START_DATE"] = df["AP_SD_SUBMIT_DATE_UT"]
    
    assert isinstance(df, pd.DataFrame)
    return df


def get_tickets_last_action_date(df, df_operations):
    """ PURE FUNCTION
    """
    assert isinstance(df, pd.DataFrame)
    assert isinstance(df_operations, pd.DataFrame)

    df_operations = df_operations.loc[df_operations["AP_AM_RFC_NUMBER"].isin(df["AP_SD_RFC_NUMBER"])]

    # affectation_tasks = ["Fin de suspension", "Installation", "Analyse et qualification", "Realisation de la demande",	
    #         "Realisation", "Installation, Parametrage et Permission", "Traitement de l\'incident",
    #         "Realisation Preparation et installation","Realisation livraison digipass",
    #         "Realisation Installation", "Traitement du refus solution", "Realisation Approvisionnement",
    #         "Redirection", "Parametrage et installation Telephone", "Refus solution", "Realisation livraison telephone",
    #         "Traitement du refus de la solution", "Installation AUTOCAD", "Validation de la solution"]

    # **	1	Validation Self Service
    # **	2	Logistique
    # **	22	Fin du Workflow
    # **	23	Etape conditionnelle
    # **	28	Clôture anticipée
    # **	42	Résolution par le parent
    # **	104	Validation Self Service avec Authentification
    # **	107	Historique des mouvements

    # TODO: C_LAST_ACTION_DATE is ambiguous. it's actually time since last action
    df_operations = df_operations.loc[:,("AP_AM_ACTION_ID", "AP_AM_REQUEST_ID", "AP_AM_ACTION_TYPE_ID", "AP_AM_START_DATE", "AP_AM_DONE_BY_OPERATOR_NAME")]
    mask_not_realisation_demande = (df_operations["AP_AM_ACTION_TYPE_ID"].isin([1,2,22,23,39,42,104,107,111]))
    df_operations = df_operations.loc[~mask_not_realisation_demande]

    df_operations = df_operations.sort_values(by = ["AP_AM_START_DATE"], ascending = True).groupby("AP_AM_REQUEST_ID").first()
    df_operations["C_LAST_ACTION_DATE"] = (pd.Timestamp.now(tz = 'Europe/Paris') - df_operations["AP_AM_START_DATE"]).dt.round("min")
    assert (df_operations["C_LAST_ACTION_DATE"] > timedelta(seconds=1)).any()

    df_operations = df_operations.loc[:, ("C_LAST_ACTION_DATE")]

    
    assert len(df_operations.index) == len(df.index)
    df = df.merge(df_operations, left_on = "AP_SD_REQUEST_ID", right_index = True, how = "left", suffixes=(False, False))

    assert isinstance(df, pd.DataFrame)
    return df


def get_ticket_age(df):
    assert isinstance(df, pd.DataFrame)
    assert "C_START_DATE" in df.columns

    df.loc[:,"C_TICKET_AGE"] = (pd.Timestamp.now(tz = 'Europe/Paris') - df["C_START_DATE"]).dt.round("min")

    assert isinstance(df, pd.DataFrame)
    return df


def get_ticket_classification(df):
    assert isinstance(df, pd.DataFrame)

    df_query = df.loc[:,("AP_SD_REQUEST_ID","AP_SD_RFC_NUMBER")]
    df_result = get_requests_intervention_types(df = df_query)

    df = df.merge(df_result, left_on = "AP_SD_REQUEST_ID", right_on = "AP_REQUEST_ID", how = "left", suffixes=(False, False))

    assert isinstance(df, pd.DataFrame)
    return df


def get_employee_movement_question_info(df):
    assert isinstance(df, pd.DataFrame)

    df.loc[:,"RESULT_STRING_FR"].fillna(df["RESULT"], inplace=True)

    def unstack_questions(group):
        return group.pivot(index="REQUEST_ID", columns="QUESTION_ID", values="RESULT_STRING_FR").reset_index(drop = True).rename_axis(None, axis=1)

    df = df.groupby(["REQUEST_ID", "RFC_NUMBER", "SD_CATALOG_ID"]).apply(unstack_questions).reset_index()
    del df["level_3"]

    return df


def get_tickets_under_observation(df, df_operations):
    assert isinstance(df, pd.DataFrame)

    df.loc[:,"C_PILOTAGE_INDICATORS"] = ""

    def find_tickets_with_tag(df_ops):
        df_ops = df_ops.loc[df_ops["AP_AM_START_DATE"] == df_ops["AP_AM_START_DATE"].max()]
        df_ops.drop_duplicates(subset="AP_AM_START_DATE", inplace=True)
        return df_ops

    df_operations = df_operations.loc[df_operations["ACTION_TYPE_ID"] == 108]
    df_operations = df_operations.groupby("AP_AM_RFC_NUMBER").apply(find_tickets_with_tag).reset_index(drop=True)
    df_operations["C_LAST_ACTION_DATE"] = (pd.Timestamp.now(tz = 'Europe/Paris') - df_operations["AP_AM_START_DATE"]).dt.round("min")
    assert (df_operations["C_LAST_ACTION_DATE"] > timedelta(seconds=1)).any()

    df_operations = df_operations.loc[:, ("AP_AM_RFC_NUMBER", "C_LAST_ACTION_DATE")]

    assert len(df_operations.index) == len(df.index)
    df = df.merge(df_operations, left_on = "AP_SD_RFC_NUMBER", right_on = "AP_AM_RFC_NUMBER", how = "left", suffixes=(False, False))


    return df
