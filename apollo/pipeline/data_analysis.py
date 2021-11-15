import re
import pandas as pd
from pandas.tseries.offsets import BDay


def get_suspended_mail_not_recontacted(df, df_operations):
    assert isinstance(df, pd.DataFrame)
    assert isinstance(df_operations, pd.DataFrame)
    
    mask_status = (df["AP_SD_STATUS_ID"].isin([39, 42]))
    df = df.loc[mask_status]

    if not df.empty:
        df_operations = df_operations.loc[df_operations["AP_AM_RFC_NUMBER"].isin(df["AP_SD_RFC_NUMBER"])]

        def get_operations_contacted_by_mail_and_last_action(df_ops):
            print(df_ops)
            regex_notification_task = "Appel sortant utilisateur|Notification au demandeur|Relance vers l'utilisateur|Envoi de mail au demandeur"
            df_ops = df_ops[df_ops["AP_AM_OPERATION_TYPE_NAME"].str.contains(regex_notification_task, flags=re.IGNORECASE, regex=True)]

            if df_ops.empty:
                return pd.Series(data=[True], index=["suspended_mail_not_recontacted"])

            else:
                df_ops["date"] = df_ops["AP_AM_START_DATE"].dt.date
                df_ops = df_ops.sort_values("AP_AM_START_DATE", ascending = False).drop_duplicates(subset = "date", keep = "first")
                no_of_contacts = len(df_ops.index)
                
                df_last_contact = df_ops.loc[df_ops["AP_AM_START_DATE"] == df_ops["AP_AM_START_DATE"].max()]
                df_last_contact.drop_duplicates(subset="AP_AM_START_DATE", inplace=True)

                if (no_of_contacts >= 3) & (pd.Timestamp.now(tz = 'Europe/Paris') > (df_last_contact["AP_AM_START_DATE"] + 3*BDay())).bool():
                    return pd.Series(data=[True], index=["suspended_mail_not_recontacted"])
                elif (no_of_contacts == 2) & (pd.Timestamp.now(tz = 'Europe/Paris') > (df_last_contact["AP_AM_START_DATE"] + 2*BDay())).bool():
                    return pd.Series(data=[True], index=["suspended_mail_not_recontacted"])
                elif (no_of_contacts == 1) & (pd.Timestamp.now(tz = 'Europe/Paris') > (df_last_contact["AP_AM_START_DATE"] + 1*BDay())).bool():
                    return pd.Series(data=[True], index=["suspended_mail_not_recontacted"])
                else:
                    return pd.Series(data=[False],index=["suspended_mail_not_recontacted"])

        df_operations = df_operations.groupby("AP_AM_RFC_NUMBER").apply(get_operations_contacted_by_mail_and_last_action)
        
        df = df.merge(df_operations, left_on="AP_SD_RFC_NUMBER", right_on = "AP_AM_RFC_NUMBER", how = "left", suffixes=(False, False))

    if not df.empty:
        df = df.loc[df["suspended_mail_not_recontacted"] == True]
        df.drop(columns=["suspended_mail_not_recontacted"], inplace = True)

    assert isinstance(df, pd.DataFrame)
    return df


def get_contacted_x_times(df, df_operations, no_of_min_contacts):
    assert isinstance(df, pd.DataFrame)
    assert isinstance(df_operations, pd.DataFrame)
    assert isinstance(no_of_min_contacts, int)

    def get_operations_contacted_x_times(df_ops):
        regex_notification_task = "Appel sortant utilisateur|Notification au demandeur|Relance vers l'utilisateur|Envoi de mail au demandeur"
        df_ops = df_ops[df_ops["AP_AM_OPERATION_TYPE_NAME"].str.contains(regex_notification_task, flags=re.IGNORECASE, regex=True)]
        df_ops["date"] = df_ops["AP_AM_START_DATE"].dt.date
        df_ops = df_ops.sort_values("AP_AM_START_DATE", ascending = False).drop_duplicates(subset = "date", keep = "first")

        no_of_contacts = len(df_ops)

        if (no_of_contacts >= no_of_min_contacts):
            first_contact_date = df_ops["AP_AM_START_DATE"].min()
            last_contact_date = df_ops["AP_AM_START_DATE"].max()
            bdays_since_creation_and_last_contact = len(pd.date_range(start = first_contact_date, end = last_contact_date, freq = BDay()))
            if bdays_since_creation_and_last_contact >= 7:
                return pd.Series(data=[True], index=["contacted_by_mail_x_times"])
            else:
                return pd.Series(data=[False], index=["contacted_by_mail_x_times"])
        else:
            return pd.Series(data=[False], index=["contacted_by_mail_x_times"])

    mask_status = (df["AP_SD_STATUS_ID"].isin([39,42]))
    mask_priority = (df["AP_SD_URGENCY_ID"] == 5)
    df = df.loc[mask_status & mask_priority]

    if not df.empty:
        df_operations = df_operations.loc[df_operations["AP_AM_RFC_NUMBER"].isin(df["AP_SD_RFC_NUMBER"])]
        df_operations = df_operations.groupby("AP_AM_RFC_NUMBER").apply(get_operations_contacted_x_times)
        df = df.merge(df_operations, left_on="AP_SD_RFC_NUMBER", right_on = "AP_AM_RFC_NUMBER", how = "left", suffixes=(False, False))

    if not df.empty:
        df = df.loc[df["contacted_by_mail_x_times"] == True]
        df.drop(columns=["contacted_by_mail_x_times"], inplace = True)

    assert isinstance(df, pd.DataFrame)
    return df


def get_av_security_tickets(df):
    """ PURE FUNCTION
    """
    assert isinstance(df, pd.DataFrame)

    mask_ci = (df["AP_SD_CI_ID"].isin([9926, 8696]))
    description_regex = "(anti)?virus|^vol.?$|pirat(é|e)|malware|(mc)?afee"
    mask_description = (df["AP_SD_COMMENT"].str.contains(description_regex,
                                                       regex=True,
                                                       flags=re.IGNORECASE,
                                                       na=False))

    df_securite = df.loc[mask_ci | mask_description]

    assert isinstance(df_securite, pd.DataFrame)
    return df_securite


def get_vip_tickets(df, vip_list):
    """ PURE FUNCTION
    """
    assert isinstance(df, pd.DataFrame)

    mask_requestor = (df["REQUESTOR_LAST_NAME"].isin(vip_list))
    mask_beneficiary = (df["AP_SD_RECIPIENT_LAST_NAME"].isin(vip_list))
    df_vip = df.loc[mask_requestor | mask_beneficiary]

    assert isinstance(df_vip, pd.DataFrame)
    return df_vip


def get_sensitive_personnel_tickets(df, sensitive_personnel_list):
    """ PURE FUNCTION
    """
    assert isinstance(df, pd.DataFrame)

    mask_requestor = (df["REQUESTOR_LAST_NAME"].isin(sensitive_personnel_list))
    mask_beneficiary = (df["AP_SD_RECIPIENT_LAST_NAME"].isin(sensitive_personnel_list))
    df_result = df.loc[mask_requestor | mask_beneficiary]

    assert isinstance(df_result, pd.DataFrame)
    return df_result
  

def get_industrial_tickets(df):
    """ PURE FUNCTION
    """
    assert isinstance(df, pd.DataFrame)

    industrial_ci = ["POSTES INDUSTRIELS_ENV", "Mustang_Mobilité_ENV"]
    mask_ci = (df["AP_SD_CI_NAME"].isin(industrial_ci))
    description_regex = "scada|tablette|conduite"
    mask_description = (df["AP_SD_COMMENT"].str.contains(description_regex,
                                                       regex=True,
                                                       flags=re.IGNORECASE,
                                                       na=False))

    df_industrial = df.loc[mask_ci | mask_description]

    assert isinstance(df_industrial, pd.DataFrame)
    return df_industrial


def _assign_pts_per_day_since_creation(df):
    """ 10/100/1000 pts base + 1/10/100/1000 pts/jour pour tickets en Normal/Sensible/Important/Majeur since creation date
    """
    assert isinstance(df, pd.DataFrame)
    assert "C_TICKET_AGE" in df.columns
    assert "C_TICKET_TYPE" in df.columns

    df.loc[:,"C_POINTS"] = 0
    df.loc[:,"C_POINTS_JUSTIFICATION"] = ""

    mask_normal_urgency = (df["AP_SD_URGENCY_ID"] == 5)
    mask_sensible_urgency = (df["AP_SD_URGENCY_ID"] == 3)
    mask_important_urgency = (df["AP_SD_URGENCY_ID"] == 2)
    mask_majeur_urgency = (df["AP_SD_URGENCY_ID"] == 1)

    mask_incident = (df["C_TICKET_TYPE"] == 0)
    mask_request = (df["C_TICKET_TYPE"] == 1)

    df.loc[mask_normal_urgency & mask_incident, "C_POINTS"] = 1
    df.loc[mask_normal_urgency & mask_request, "C_POINTS"] += df["C_TICKET_AGE"].dt.days * 1
    df.loc[mask_normal_urgency & mask_incident, "C_POINTS"] += df["C_TICKET_AGE"].dt.days * 3
    df.loc[mask_normal_urgency & mask_request, "C_POINTS_JUSTIFICATION"] = "+1 pt/jour pour Demande-Normal depuis creation"
    df.loc[mask_normal_urgency & mask_incident, "C_POINTS_JUSTIFICATION"] = "+3 pt/jour pour Incident-Normal depuis creation"

    df.loc[mask_sensible_urgency & mask_request, "C_POINTS"] = 3
    df.loc[mask_sensible_urgency & mask_incident, "C_POINTS"] = 10
    df.loc[mask_sensible_urgency & mask_request, "C_POINTS"] += df["C_TICKET_AGE"].dt.days * 3
    df.loc[mask_sensible_urgency & mask_incident, "C_POINTS"] += df["C_TICKET_AGE"].dt.days * 10
    df.loc[mask_sensible_urgency & mask_request, "C_POINTS_JUSTIFICATION"] = "3 base + 3 pt/jour pour Demande-Sensible depuis creation"
    df.loc[mask_sensible_urgency & mask_incident, "C_POINTS_JUSTIFICATION"] = "10 base + 10 pt/jour pour Incident-Sensible depuis creation"

    df.loc[mask_important_urgency & mask_request, "C_POINTS"] = 30
    df.loc[mask_important_urgency & mask_incident, "C_POINTS"] = 100
    df.loc[mask_important_urgency & mask_request, "C_POINTS"] += df["C_TICKET_AGE"].dt.days * 30
    df.loc[mask_important_urgency & mask_incident, "C_POINTS"] += df["C_TICKET_AGE"].dt.days * 100
    df.loc[mask_important_urgency & mask_request, "C_POINTS_JUSTIFICATION"] = "30 base + 30 pt/jour pour Demande-Important depuis creation"
    df.loc[mask_important_urgency & mask_incident, "C_POINTS_JUSTIFICATION"] = "100 base + 100 pt/jour pour Incident-Important depuis creation"

    df.loc[mask_majeur_urgency & mask_request, "C_POINTS"] = 300
    df.loc[mask_majeur_urgency & mask_incident, "C_POINTS"] = 1000
    df.loc[mask_majeur_urgency & mask_request, "C_POINTS"] += df["C_TICKET_AGE"].dt.days * 300
    df.loc[mask_majeur_urgency & mask_incident, "C_POINTS"] += df["C_TICKET_AGE"].dt.days * 1000
    df.loc[mask_majeur_urgency & mask_request, "C_POINTS_JUSTIFICATION"] = "300 base + 300 pt/jour pour Demande-Majeur depuis creation"
    df.loc[mask_majeur_urgency & mask_incident, "C_POINTS_JUSTIFICATION"] = "1000 base + 1000 pt/jour pour Incident-Majeur depuis creation"

    assert isinstance(df, pd.DataFrame)
    return df


def _assign_pts_per_day_since_last_action(df):
    """ 10/100/1000 pts base + 1/10/100/1000 pts/jour pour tickets en Normal/Sensible/Important/Majeur since last action
    """
    assert isinstance(df, pd.DataFrame)
    assert "C_TICKET_TYPE" in df.columns
    assert "C_LAST_ACTION_DATE" in df.columns

    df.loc[:,"C_POINTS"] = 0
    df.loc[:,"C_POINTS_JUSTIFICATION"] = ""

    mask_normal_urgency = (df["AP_SD_URGENCY_ID"] == 5)
    mask_sensible_urgency = (df["AP_SD_URGENCY_ID"] == 3)
    mask_important_urgency = (df["AP_SD_URGENCY_ID"] == 2)
    mask_majeur_urgency = (df["AP_SD_URGENCY_ID"] == 1)

    mask_incident = (df["C_TICKET_TYPE"] == 0)
    mask_request = (df["C_TICKET_TYPE"] == 1)

    df.loc[mask_normal_urgency & mask_request, "C_POINTS"] = df["C_LAST_ACTION_DATE"].dt.days
    df.loc[mask_normal_urgency & mask_incident, "C_POINTS"] = df["C_LAST_ACTION_DATE"].dt.days * 3
    df.loc[mask_normal_urgency & mask_request, "C_POINTS_JUSTIFICATION"] = "+1 pt/jour pour Demande-Normal depuis dernière action"
    df.loc[mask_normal_urgency & mask_incident, "C_POINTS_JUSTIFICATION"] = "+3 pt/jour pour Incident-Normal depuis dernière action"

    df.loc[mask_sensible_urgency & mask_request, "C_POINTS"] += df["C_LAST_ACTION_DATE"].dt.days * 3
    df.loc[mask_sensible_urgency & mask_incident, "C_POINTS"] += df["C_LAST_ACTION_DATE"].dt.days * 10
    df.loc[mask_sensible_urgency & mask_request, "C_POINTS_JUSTIFICATION"] = "+3 pt/jour pour Demande-Sensible depuis dernière action"
    df.loc[mask_sensible_urgency & mask_incident, "C_POINTS_JUSTIFICATION"] = "+10 pt/jour pour Incident-Sensible depuis dernière action"

    df.loc[mask_important_urgency & mask_request, "C_POINTS"] += df["C_LAST_ACTION_DATE"].dt.days * 30
    df.loc[mask_important_urgency & mask_incident, "C_POINTS"] += df["C_LAST_ACTION_DATE"].dt.days * 100
    df.loc[mask_important_urgency & mask_request, "C_POINTS_JUSTIFICATION"] = "+30 pt/jour pour Demande-Important depuis dernière action"
    df.loc[mask_important_urgency & mask_incident, "C_POINTS_JUSTIFICATION"] = "+100 pt/jour pour Incident-Important depuis dernière action"

    df.loc[mask_majeur_urgency & mask_request, "C_POINTS"] += df["C_LAST_ACTION_DATE"].dt.days * 300
    df.loc[mask_majeur_urgency & mask_incident, "C_POINTS"] += df["C_LAST_ACTION_DATE"].dt.days * 1000
    df.loc[mask_majeur_urgency & mask_request, "C_POINTS_JUSTIFICATION"] = "+300 pt/jour pour Demande-Majeur depuis dernière action"
    df.loc[mask_majeur_urgency & mask_incident, "C_POINTS_JUSTIFICATION"] = "+1000 pt/jour pour Incident-Majeur depuis dernière action"

    assert isinstance(df, pd.DataFrame)
    return df


def _assign_pts_per_day_not_suspended(df):
    """ 10 pts/day for not suspended
    """
    assert isinstance(df, pd.DataFrame)
    assert "C_TICKET_TYPE" in df.columns
    assert "C_LAST_ACTION_DATE" in df.columns

    df.loc[:,"C_POINTS"] = 0
    df.loc[:,"C_POINTS_JUSTIFICATION"] = ""

    df = df.loc[~df["AP_SD_STATUS_ID"].isin([5,20,39])]

    mask_normal_urgency = (df["AP_SD_URGENCY_ID"] == 5)
    mask_sensible_urgency = (df["AP_SD_URGENCY_ID"] == 3)
    mask_important_urgency = (df["AP_SD_URGENCY_ID"] == 2)
    mask_majeur_urgency = (df["AP_SD_URGENCY_ID"] == 1)

    mask_incident = (df["C_TICKET_TYPE"] == 0)
    mask_request = (df["C_TICKET_TYPE"] == 1)

    df.loc[mask_normal_urgency & mask_request, "C_POINTS"] = df["C_LAST_ACTION_DATE"].dt.days * 10
    df.loc[mask_normal_urgency & mask_incident, "C_POINTS"] = df["C_LAST_ACTION_DATE"].dt.days * 25
    df.loc[mask_normal_urgency & mask_request, "C_POINTS_JUSTIFICATION"] = "+10 pt/jour pour Demande-Normal non suspendu"
    df.loc[mask_normal_urgency & mask_incident, "C_POINTS_JUSTIFICATION"] = "+25 pt/jour pour Incident-Normal non suspendu"

    df.loc[mask_sensible_urgency & mask_request, "C_POINTS"] += df["C_LAST_ACTION_DATE"].dt.days * 25
    df.loc[mask_sensible_urgency & mask_incident, "C_POINTS"] += df["C_LAST_ACTION_DATE"].dt.days * 50
    df.loc[mask_sensible_urgency & mask_request, "C_POINTS_JUSTIFICATION"] = "+25 pt/jour pour Demande-Sensible non suspendu"
    df.loc[mask_sensible_urgency & mask_incident, "C_POINTS_JUSTIFICATION"] = "+50 pt/jour pour Incident-Sensible non suspendu"

    df.loc[mask_important_urgency & mask_request, "C_POINTS"] += df["C_LAST_ACTION_DATE"].dt.days * 50
    df.loc[mask_important_urgency & mask_incident, "C_POINTS"] += df["C_LAST_ACTION_DATE"].dt.days * 100
    df.loc[mask_important_urgency & mask_request, "C_POINTS_JUSTIFICATION"] = "+50 pt/jour pour Demande-Important non suspendu"
    df.loc[mask_important_urgency & mask_incident, "C_POINTS_JUSTIFICATION"] = "+100 pt/jour pour Incident-Important non suspendu"

    df.loc[mask_majeur_urgency & mask_request, "C_POINTS"] += df["C_LAST_ACTION_DATE"].dt.days * 300
    df.loc[mask_majeur_urgency & mask_incident, "C_POINTS"] += df["C_LAST_ACTION_DATE"].dt.days * 1000
    df.loc[mask_majeur_urgency & mask_request, "C_POINTS_JUSTIFICATION"] = "+300 pt/jour pour Demande-Majeur non suspendu"
    df.loc[mask_majeur_urgency & mask_incident, "C_POINTS_JUSTIFICATION"] = "+1000 pt/jour pour Incident-Majeur non suspendu"

    assert isinstance(df, pd.DataFrame)
    return df


def _assign_pts_per_day_vip_tickets(df):
    assert isinstance(df, pd.DataFrame)
    assert "C_LAST_ACTION_DATE" in df.columns

    df.loc[:, ("C_POINTS")] = 500
    df.loc[:, ("C_POINTS")] += df["C_LAST_ACTION_DATE"].dt.days * 100

    df.loc[:, ("C_POINTS_JUSTIFICATION")] = "1000 base + 100 pts/day ticket VIP depuis dernière action "

    assert isinstance(df, pd.DataFrame)
    return df


def _assign_points_industrial_ticket(df):
    assert isinstance(df, pd.DataFrame)

    df.loc[:, ("C_POINTS")] = 500
    df.loc[:, ("C_POINTS_JUSTIFICATION")] = "500 points pour poste industriel"

    assert isinstance(df, pd.DataFrame)
    return df


def calculate_ticket_flow(df, df_operations):
    from apollo.main import site_settings

    assert isinstance(df, pd.DataFrame)
    assert isinstance(df_operations, pd.DataFrame)

    def _append_points(df1, df2):
        assert "C_POINTS" in df1.columns
        assert "C_POINTS_JUSTIFICATION" in df1.columns
        assert "C_POINTS" in df2.columns
        assert "C_POINTS_JUSTIFICATION" in df2.columns

        df2 = df2.loc[:, ["AP_SD_RFC_NUMBER", "C_POINTS", "C_POINTS_JUSTIFICATION"]]
        df1 = df1.join(df2.set_index("AP_SD_RFC_NUMBER"),
                       on="AP_SD_RFC_NUMBER",
                       how="left",
                       rsuffix="_df2")

        df1["C_POINTS_df2"].fillna(0, inplace=True)
        df1.loc[:, ("C_POINTS")] += df1["C_POINTS_df2"]

        df1["C_POINTS_JUSTIFICATION_df2"].fillna("", inplace=True)
        df1.loc[:, ("C_POINTS_JUSTIFICATION")] = df1["C_POINTS_JUSTIFICATION"].str.cat(df1["C_POINTS_JUSTIFICATION_df2"], sep=" // ")
        del df1["C_POINTS_df2"]
        del df1["C_POINTS_JUSTIFICATION_df2"]

        return df1

    df_points = df.copy()
    df_points["C_POINTS"] = 0
    df_points["C_POINTS_JUSTIFICATION"] = ""

    # 10/100/1000 pts base + 10/100/1000 pts/jour pour tickets en Sensible/Important/Majeur since creation date
    points_per_day = _assign_pts_per_day_since_creation(df)
    df_points = _append_points(df_points, points_per_day)

    # 10/100/1000 pts base + 10/100/1000 pts/jour pour tickets en Sensible/Important/Majeur since last action
    points_since_last_action = _assign_pts_per_day_since_last_action(df)
    df_points = _append_points(df_points, points_since_last_action)

    # 10pts/day for not suspended
    points_not_suspended = _assign_pts_per_day_not_suspended(df)
    df_points = _append_points(df_points, points_not_suspended)

    # 1000 points AV / Security tickets
    points_security = get_av_security_tickets(df)
    assert "C_LAST_ACTION_DATE" in points_security
    assert "C_TICKET_AGE" in points_security
    points_security.loc[:, ("C_POINTS")] = 1000
    points_security.loc[:, ("C_POINTS_JUSTIFICATION")] = "+1000 pts ticket sécurité "
    df_points = _append_points(df_points, points_security)

    # 1000 points VIP tickets
    points_vip_tickets = _assign_pts_per_day_vip_tickets(get_vip_tickets(df=df,vip_list = site_settings.VIP_LIST))
    df_points = _append_points(df_points, points_vip_tickets)

    # 500 for poste industriel
    points_poste_industriel = _assign_points_industrial_ticket(get_industrial_tickets(df = df))
    df_points = _append_points(df_points, points_poste_industriel)

    df_points["C_POINTS"] = df_points["C_POINTS"].astype(int)

    assert isinstance(df_points, pd.DataFrame)
    return df_points


def get_not_suspended_incidents(df):
    assert isinstance(df, pd.DataFrame)

    df = df.loc[~df["AP_SD_STATUS_ID"].isin([5,20,39,42])]

    assert isinstance(df, pd.DataFrame)
    return df


def get_ticket_flow(df, inter_type = None, suspended = None):
    assert isinstance(df, pd.DataFrame)

    if suspended is False:
        df = df.loc[~df["AP_SD_STATUS_ID"].isin([5,20,39,42])]
    
    # TODO check if it's not worth it to precalculate if it's hotline or proxi
    if inter_type == "hotline":
        return df.loc[df["AP_INTERVENTION_TYPE"] == 2]

    elif inter_type == "proxy":
        return df.loc[df["AP_INTERVENTION_TYPE"].isin([3,4,5,6,7,8,9,10,11,12,13,14,15,16])]

    elif inter_type == "unclassified":
        return df.loc[df["AP_INTERVENTION_TYPE"] == 1]

    elif inter_type == "incident":
        return df.loc[df["C_TICKET_TYPE"] == 0]

    elif inter_type == "request":
        return df.loc[df["C_TICKET_TYPE"] == 1]

    elif inter_type == "incident+hotline":
        mask_incident = (df["C_TICKET_TYPE"] == 0)
        mask_hotline = (df["AP_INTERVENTION_TYPE"] == 2)
        return df.loc[mask_incident & mask_hotline]

    elif inter_type == "incident+proxy":
        mask_incident = (df["C_TICKET_TYPE"] == 0)
        mask_proxi = (df["AP_INTERVENTION_TYPE"].isin([3,4,5,6,7,8,9,10,11,12,13,14,15,16]))
        return df.loc[mask_incident & mask_proxi]

    elif inter_type == "request+hotline":
        mask_request = (df["C_TICKET_TYPE"] == 1)
        mask_hotline = (df["AP_INTERVENTION_TYPE"] == 2)
        return df.loc[mask_request & mask_hotline]

    elif inter_type == "request+proxy":
        mask_request = (df["C_TICKET_TYPE"] == 1)
        mask_proxi = (df["AP_INTERVENTION_TYPE"].isin([3,4,5,6,7,8,9,10,11,12,13,14,15,16]))
        return df.loc[mask_request & mask_proxi]

    else:
        return df


def get_suspended_gt_x(df, df_operations, inter_type_filter):
    assert isinstance(df, pd.DataFrame)
    assert isinstance(df_operations, pd.DataFrame)
    assert "AP_INTERVENTION_TYPE" in df.columns

    def get_delta_since_suspended(df_ops):
        suspension_date = df_ops.loc[df_ops["AP_AM_ACTION_TYPE_ID"] == 5, "AP_AM_START_DATE"].max()
        if inter_type_filter == "hotline":
            if (suspension_date + 1*BDay()) < pd.Timestamp.now(tz = 'Europe/Paris'):
                return pd.Series(data=[True], index=["suspended_gt_x"])
            else:
                return pd.Series(data=[False], index=["suspended_gt_x"])

        else:
            if (suspension_date + 5*BDay()) < pd.Timestamp.now(tz = 'Europe/Paris'):
                return pd.Series(data=[True], index=["suspended_gt_x"])
            else:
                return pd.Series(data=[False], index=["suspended_gt_x"])

    df = df.loc[df["AP_SD_STATUS_ID"].isin([5,20])]
    if inter_type_filter == "hotline":
        df = df.loc[df["AP_INTERVENTION_TYPE"].isin([2])]
    else:
        df = df.loc[df["AP_INTERVENTION_TYPE"].isin([3,4,5,6,7,8,9,10,11,12,13,14,15,16])]
    

    if not df.empty:
        df_operations = df_operations.loc[df_operations["AP_AM_RFC_NUMBER"].isin(df["AP_SD_RFC_NUMBER"])]
        df_operations = df_operations.groupby("AP_AM_RFC_NUMBER").apply(get_delta_since_suspended)
        df = df.merge(df_operations, left_on="AP_SD_RFC_NUMBER", right_on = "AP_AM_RFC_NUMBER", how = "left", suffixes=(False, False))
        df = df.loc[df["suspended_gt_x"] == True]
        df.drop(columns=["suspended_gt_x"], inplace = True)

        assert isinstance(df, pd.DataFrame)
        return df
    else:
        assert isinstance(df, pd.DataFrame)
        return df


def get_tickets_software_install(df):
    """ PURE FUNCTION
    """
    assert isinstance(df, pd.DataFrame)

    mask_ci = (df["AP_SD_CATALOG_ID"].isin([5687, 5564, 5573, 5577, 5578, 5580, 5581, 5582, 5583, 5584, 5593, 5594, 5595,
        5597, 5598, 5599, 5600, 5601, 5602, 5603, 5604, 5606, 5607, 5608, 5793, 5609, 5610, 5611, 5613, 5614, 5729, 5616,
        5680, 5398, 5686, 5691, 5690, 5840, 5757, 5758, 5759, 5760, 5796,5836, 5837, 5846, 5617]))
    df = df.loc[mask_ci]

    assert isinstance(df, pd.DataFrame)
    return df


def get_new_arrival_tickets(df):
    """ PURE FUNCTION
    """
    assert isinstance(df, pd.DataFrame)

    df = df.loc[df["AP_SD_CATALOG_ID"] == 5535]

    return df


def get_higher_3_tickets(df):
    """ PURE FUNCTION
    """
    assert isinstance(df, pd.DataFrame)

    df_urgent_tickets = df.loc[df["AP_SD_URGENCY_ID"].isin([2,1])]

    assert isinstance(df_urgent_tickets, pd.DataFrame)
    return df_urgent_tickets


def get_digipass_tickets(df):
    """ PURE FUNCTION
    """
    assert isinstance(df, pd.DataFrame)

    digipass_tickets = ["DIGIPASS", "TELETRAVAIL_ENV", "Azure.Microsoft"]
    mask_ci = (df["AP_SD_CI_NAME"].isin(digipass_tickets))

    regex_digipass = "big.?ip|vpn"
    mask_regex = (df["AP_SD_COMMENT"].str.contains(regex_digipass, flags=re.IGNORECASE, regex=True))


    df_digipass = df.loc[mask_ci | mask_regex]

    assert isinstance(df_digipass, pd.DataFrame)
    return df_digipass


def get_skype_tickets(df):
    """ PURE FUNCTION
    """
    assert isinstance(df, pd.DataFrame)

    df_skype = df.loc[df["AP_SD_CI_NAME"] == "SKYPE_ENV"]

    assert isinstance(df_skype, pd.DataFrame)
    return df_skype


def get_outlook_tickets(df):
    """ PURE FUNCTION
    Gets all the tickets concerning Outlook (local and web),
    including mail spam (FILTRAGE_MAIL_ENV)
    """
    assert isinstance(df, pd.DataFrame)

    outlook_ci = [
        "OUTLOOK_APP", "MESSAGERIE_ENV", "WEBMAIL_APP", "FILTRAGE_MAIL_ENV",
        "OFFICE_APP"
    ]
    df_outlook = df.loc[df["AP_SD_CI_NAME"].isin(outlook_ci)]

    assert isinstance(df_outlook, pd.DataFrame)
    return df_outlook


def get_telephone_tickets(df):
    """ PURE FUNCTION
    """
    assert isinstance(df, pd.DataFrame)

    telephone_ci = ["TELEPHONIE_MOBILE_ENV", "TELEPHONIE_ENV"]
    df_telephone = df.loc[df["AP_SD_CI_NAME"].isin(telephone_ci)]

    assert isinstance(df_telephone, pd.DataFrame)
    return df_telephone


def get_under_observation_tickets(df, df_operations):
    """ PURE FUNCTION
    """
    assert isinstance(df, pd.DataFrame)
    assert isinstance(df_operations, pd.DataFrame)

    mask_action_type = (df_operations["AP_AM_ACTION_TYPE_ID"].isin([65, 108]))
    mask_not_null = (~df_operations["AP_AM_DESCRIPTION"].isnull())

    filter_actions = df_operations.loc[mask_action_type & mask_not_null]
    mask_tagp_match = (filter_actions["AP_AM_DESCRIPTION"].str.contains("#tagp# sousobservation"))
    filter_under_observation = filter_actions.loc[mask_tagp_match]["AP_AM_REQUEST_ID"].unique()

    df = df.loc[df["AP_SD_REQUEST_ID"].isin(filter_under_observation)]

    assert isinstance(df, pd.DataFrame)
    return df


def get_rdv_date_state(df, df_operations):
    """ PURE FUNCTION
    """
    assert isinstance(df, pd.DataFrame)
    assert isinstance(df_operations, pd.DataFrame)

    mask_action_type = (df_operations["AP_AM_ACTION_TYPE_ID"].isin([65, 108]))
    mask_not_null = (~df_operations["AP_AM_DESCRIPTION"].isnull())

    filter_actions = df_operations.loc[mask_action_type & mask_not_null]
    mask_tagp_match = (filter_actions["AP_AM_DESCRIPTION"].str.contains("#tagp# rdv:"))
    filter_rdv_present = filter_actions.loc[mask_tagp_match, ("AP_AM_REQUEST_ID","AP_AM_DESCRIPTION")]

    df["C_RDV_STATE"] = "Pas de RDV"
    if not filter_rdv_present.empty:
        filter_rdv_present["C_RDV_DATE"] = filter_rdv_present["AP_AM_DESCRIPTION"].str.extract(r'((?<=:)[0-9]{2}.[0-9]{2}.[0-9]{4})', expand = False)
        print(filter_rdv_present)
        filter_rdv_present["C_RDV_DATE"] = pd.to_datetime(filter_rdv_present["C_RDV_DATE"], errors = "coerce", format = "%d/%m/%Y", utc = True)
        filter_rdv_present["C_RDV_DATE"] = filter_rdv_present["C_RDV_DATE"].dt.tz_convert('Europe/Paris')
        filter_rdv_present = filter_rdv_present.sort_values(["AP_AM_REQUEST_ID", "C_RDV_DATE"], ascending = False) \
                                                .drop_duplicates(subset = "AP_AM_REQUEST_ID", keep = "first")

        df = df.merge(filter_rdv_present, left_on="AP_SD_REQUEST_ID", right_on = "AP_AM_REQUEST_ID", how = "left", suffixes=(False, False))   
     
        mask_nul_rdv = df["C_RDV_DATE"].isnull()
        mask_rdv_set = df["AP_SD_REQUEST_ID"].isin(filter_rdv_present["AP_AM_REQUEST_ID"])
        df.loc[(mask_nul_rdv & mask_rdv_set), ("C_RDV_STATE")] = "RDV - date invalide"

        mask_rdv_en_cours = (df["C_RDV_DATE"] >= (pd.Timestamp.now(tz = 'Europe/Paris')))
        mask_rdv_en_retard = (df["C_RDV_DATE"] <= (pd.Timestamp.now(tz = 'Europe/Paris')))
        df.loc[(~mask_nul_rdv & mask_rdv_en_cours), ("C_RDV_STATE")] = "RDV - en cours"
        df.loc[(~mask_nul_rdv & mask_rdv_en_retard), ("C_RDV_STATE")] = "RDV - en retard"

    else:
        df["C_RDV_DATE"] = ""    

    assert isinstance(df, pd.DataFrame)
    return df


def get_tickets_score_by_technician_table(df):
    total_score_by_technician = {
        "columns" : [{
            "col_order": 0,
            "name": "Technicien",
            "col_id": "tech_name"
        },
        {
            "col_order": 1,
            "name": "Total",
            "col_id": "total_tickets"
        },
        {
            "col_order": 2,
            "name": "Score",
            "col_id": "score"
        }],
        "data": []
    }

    for name, group in df.groupby("AP_AM_DONE_BY_OPERATOR_NAME"):
        if not group.empty:
            entry = {
                "tech_name": name,
                "total_tickets": len(group.index),
                "score": int(group["C_POINTS"].sum())
            }
            total_score_by_technician["data"].append(entry)

    return total_score_by_technician


def get_tickets_by_expiration_time_table(df):
    assert isinstance(df, pd.DataFrame)

    df = df.loc[~df["AP_SD_STATUS_ID"].isin([5,20,39,42])]

    df = df.loc[df["AP_SD_MAX_RESOLUTION_DATE"] > pd.Timestamp.now(tz = 'Europe/Paris')]
    df = df.loc[:,("AP_SD_RFC_NUMBER", "AP_TYPE_FR", "AP_SD_RECIPIENT_LOCATION_RH", "AP_AM_DONE_BY_OPERATOR_NAME", "AP_SD_MAX_RESOLUTION_DATE")]
    df.sort_values(by="AP_SD_MAX_RESOLUTION_DATE", inplace = True)
    df.loc[:,"AP_SD_MAX_RESOLUTION_DATE"] = df["AP_SD_MAX_RESOLUTION_DATE"].dt.strftime("%d/%m/%y %H:%M")
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

    time_remaining_by_ticket = {
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
        },
        {
            "col_order": 4,
            "name": "SLA",
            "id": "AP_SD_MAX_RESOLUTION_DATE"
        }],
        "data": []
    }

    time_remaining_by_ticket["data"] = df.to_dict("records")

    return time_remaining_by_ticket


def get_total_score(df):
    assert isinstance(df, pd.DataFrame)
    assert "C_POINTS" in df.columns

    return int(df["C_POINTS"].sum())


def get_df_len(df):
    assert isinstance(df, pd.DataFrame)

    return int(len(df))

def get_employee_mouvement(df):
    assert isinstance(df, pd.DataFrame)

    df.rename(columns = {
    45: "NOM",
    46: "PRENOM",
    48: "LOCATION",
    49: "CONTRACT",
    50: "ARRIVAL_DATE",
    55: "PC_PRESENT",
    58: "TELEPHONE",
    }, inplace = True)

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

    df["LOCATION"].replace(location_translator, inplace = True)
    df.loc[df["LOCATION"].isnull(), ("LOCATION")] = "Pas de location"

    df.loc[:,"PC"] = 0
    df.loc[df["PC_PRESENT"] == "Non", ("PC")] = 1
    del df["PC_PRESENT"]

    df.loc[df["TELEPHONE"] != "Oui", ("TELEPHONE")] = 0
    df.loc[df["TELEPHONE"] == "Oui", ("TELEPHONE")] = 1

    df.loc[:,"NOM"] = df["NOM"].str.upper()
    df.loc[:,"PRENOM"] = df["PRENOM"].str.title()

    df.loc[:,"PROXI"] = 0
    mask_tel = (df["TELEPHONE"] == 1)
    mask_pc = (df["PC"] == 1)
    df.loc[mask_tel | mask_pc, "PROXI"] = 1

    df.loc[:,"COLOR"] = "blue"
    mask_new_arrival = (df["SD_CATALOG_ID"] == 5535)
    mask_not_proxi = (df["PROXI"] == 0)
    df.loc[mask_new_arrival & mask_not_proxi, "COLOR"] = "green"
    
    df.loc[:,"TITLE"] = df["RFC_NUMBER"] + " - " + df["NOM"] + ", " + df["PRENOM"] + " - " + df["LOCATION"] + " - " + "PC:" + df["PC"].astype("str") + " Tel:" + df["TELEPHONE"].astype("str")
    df.loc[:,"ARRIVAL_DATE"] = pd.to_datetime(df["ARRIVAL_DATE"], format = "%d/%m/%Y").dt.strftime("%Y-%m-%d")

    result = [{
        "title": title,
        "start": start_date,
        "color": color,
        "extendedProps": {
            "SPOT": request_number,
            "Nom": name,
            "Prenom": surname,
            "Location": location,
            "Contract": contract,
            "Arrivée": arrival_date,
            "PC": pc,
            "Telephone": telephone
        }
    } for title, start_date, color, request_number, name, surname, location, contract, arrival_date, pc, telephone in 
        zip(df["TITLE"], df["ARRIVAL_DATE"], df["COLOR"], df["RFC_NUMBER"], df["NOM"], df["PRENOM"], df["LOCATION"], df["CONTRACT"], df["ARRIVAL_DATE"], df["PC"], df["TELEPHONE"])
    ]

    return result


def get_rdv_dates(df):
    assert isinstance(df, pd.DataFrame)
    
    mask_rdv_en_cours = (df["C_RDV_STATE"] == "RDV - en cours")
    mask_rdv_en_retard = (df["C_RDV_STATE"] == "RDV - en retard")
    df = df.loc[(mask_rdv_en_cours | mask_rdv_en_retard)]

    print(df.columns)
    print(df.info())
    df["AP_SD_RECIPIENT_LOCATION_RH"] = df["AP_SD_RECIPIENT_LOCATION_RH"].astype(str)
    df.loc[:,"TITLE"] = df["AP_SD_RFC_NUMBER"] + " - " + \
                        df["AP_SD_RECIPIENT_LAST_NAME"] + ", " + \
                        df["AP_SD_RECIPIENT_LOCATION_RH"] + " - " + \
                        df["AP_TYPE_FR"]

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

    df["AP_SD_RECIPIENT_LOCATION_RH"].replace(location_translator, inplace = True)
    df.loc[df["AP_SD_RECIPIENT_LOCATION_RH"].isnull(), ("AP_SD_RECIPIENT_LOCATION_RH")] = "Pas de location"

    df["C_RDV_DATE"] = df["C_RDV_DATE"].dt.strftime("%Y-%m-%d")
    df["COLOR"] = ""
    df.loc[df["AP_SD_RECIPIENT_LOCATION_RH"].str.startswith("DM"), "COLOR"] = "#F1C40F"
    df.loc[df["AP_SD_RECIPIENT_LOCATION_RH"].str.startswith("DHR"), "COLOR"] = "#3498DB"
    df.loc[df["AP_SD_RECIPIENT_LOCATION_RH"].str.startswith("DRS"), "COLOR"] = "#8E44AD"
    df.loc[df["AP_SD_RECIPIENT_LOCATION_RH"].str.startswith("DRI"), "COLOR"] = "#E67E22"
    df.loc[df["AP_SD_RECIPIENT_LOCATION_RH"].str.startswith("DRM"), "COLOR"] = "#27AE60"
    df.loc[df["AP_SD_RECIPIENT_LOCATION_RH"].str.startswith("Paris"), "COLOR"] = "#34495E"
    df.loc[df["AP_SD_RECIPIENT_LOCATION_RH"].str.startswith("Pas de location"), "COLOR"] = "#FC0B03"

    result = [{
        "title": title,
        "start": start_date,
        "color": color,
        "extendedProps": {
            "SPOT": request_number,
            "Nom": name,
            "Location": location,
            "Ticket type": ticket_type,
            "Description": ticket_description
        }
    } for title,
            start_date,
            color,
            request_number,
            name,
            location,
            ticket_type,
            ticket_description in 
        zip(df["TITLE"],
                df["C_RDV_DATE"],
                df["COLOR"],
                df["AP_SD_RFC_NUMBER"],
                df["AP_SD_RECIPIENT_LAST_NAME"],
                df["AP_SD_RECIPIENT_LOCATION_RH"],
                df["AP_TYPE_FR"],
                df["AP_SD_COMMENT"])
    ]

    return result