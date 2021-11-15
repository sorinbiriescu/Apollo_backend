import os
import time
from datetime import datetime
import uuid
from typing import Callable, Tuple, Union

import pandas as pd
from fastapi.logger import logger as log
from pymemcache.client.base import Client
from sqlalchemy.engine.base import Engine
import asyncio

current_working_directory = os.getcwd()

async def get_requests(engine: Callable[[], Engine], memcached_client: Client, lock) -> Tuple[pd.DataFrame, str]:
    ''' Creates a connexion with SPOT SQL server and executes query

    Args:
        engine: the function which returns the sqlalchemy connexion

    Returns:
        df_tickets: dataframe with the results

            The fields in the result
            * REQUEST_ID: unique id from the SPOT DB
            * PARENT_REQUEST_ID: request id of parent if current request is a child
            * RFC_NUMBER: unique number in format (I/D)YYMM1234
            * CREATION_DATE_UT: creation data in the DB
            * SUBMIT_DATE_UT: submission date of the request
            * END_DATE_UT: date of closure of ticket
            * MAX_RESOLUTION_DATE: SLA of the ticket
            * LAST_UPDATE: 
            * SUBMITTED_BY: id of the person who created the ticket
            * SUBMITTED_BY_LAST_NAME: name of the person who created the ticket
            * REQUESTOR_ID: id of who ordered the creation of the ticket, the equivalent of demandeur in SPOT
            * REQUESTOR_LAST_NAME : name of who ordered the creation of the ticket, the equivalent of demandeur in SPOT
            * REQUESTOR_LOCATION_RH: location of the person who ordered the ticket
            * LOCATION_ID: location id of the person who ordered the ticket
            * REQUESTOR_PHONE: phone of the person who ordered the ticket
            * RECIPIENT_ID: id of the person impacted by the ticket, equivalent of beneficiary in SPOT
            * RECIPIENT_LAST_NAME: name of the person impacted by the ticket, equivalent of beneficiary in SPOT
            * RECIPIENT_LOCATION_RH: location of the person impacted by the ticket, equivalent of beneficiary in SPOT
            * SD_CATALOG_ID: id of the type of request
                ** see SD_CATALOG.csv in the website documentation
            * CATALOG_NAME: name of the type of request
            * STATUS_ID: id of the status of the ticket
                ** 1	Planifié
                ** 2	Résolu
                ** 5	Suspendu
                ** 9	En attente de validation hierarchique
                ** 12	Nouveau
                ** 13	En attente d'installation
                ** 20	Suspendu en cours de préparation
                ** 23	En attente de validation budgétaire
                ** 24	Validée
                ** 36	Redirigé vers un niveau inférieur
                ** 39	Suspendu en attente de réponse mail
            * STATUS_FR: name of the status of the ticket
            * COMMENT: description of the ticket
            * DESCRIPTION: 2nd description ???
            * URGENCY_ID: id of the urgency of the ticket. CNR doesn't use the combination of urgency and impact, so
                URGENCY_ID is actually the priority of the ticket
                ** 5    Normal
                ** 3    Sensible
                ** 2    Important
                ** 1    Majeur
            * URGENCY_FR: name of the urgency of the ticket
            * CI_ID: id of the CI
            * CI_NAME: name of the CI
            * OWNER_ID: id of the operator who has the ticket ??? not sure how it works for requests with parallel tasks
            * OWNER_LAST_NAME: name of the operator who has the ticket ???
            * OWNING_GROUP_ID: id of the group who has the ticket ??? not sure how it works for requests with parallel tasks
                ** 5: Hotline
                ** 8: SUP_DSI_PILOTAGE
                ** 9: Astreinte
                ** 111: SUP_DSI
                ** 532: RESP_Hotline
                ** 533: GESTIONNAIRE_Contrat
                ** 112: CAB
            * OWNING_GROUP_NAME: name of the group who has the ticket ???
            * DEPARTMENT_ID: id of the department of the requestor ? recipient ? owner ?
            * REQUESTOR_FEEDBACK: ???
            * ASSET_ID: id of the asset (ex: computer) concerned by the ticket
            * ASSET_TAG: name of the asset (ex: computer) concerned by the ticket
            * FIRST_CALL_RESOLUTION: if the ticket was closed immediately after creation
            * SEVERITY_ID: ???
            * TIME_USED_TO_SOLVE_REQUEST: self explanatory
            * REQUEST_ORIGIN_ID: REQUEST_ORIGIN_ID,
            * DELAY: ??? not sure how it relates to TIME_USED_TO_SOLVE_REQUEST
            * LAST_GROUP_ID: id of the last group who intervened on the ticket ???
            * LAST_DONE_BY_ID: id of the person who last intervened on the ticket ???
            * SLA_ID: id of the SLA applied to the ticket
            * IMPACT_ID: ??? (seems not to be used as in a normal ITIL format)
            * E_INFRA_COMMENT: ??? sometimes there's some info marked here from certain infra interventions
            * IS_MAJOR_INCIDENT: ??? apparently not used, could not find in the interface where can you mark
                a ticket as major
            * E_TYPE_SUPPORT: external field added to classify tickets. Only works for incidents, not configured
                for requests
    '''

    from apollo.main import site_settings

    requests_sql_query: str = '''SELECT TOP (1000) [SD_REQUEST].[REQUEST_ID]
                ,[SD_REQUEST].[PARENT_REQUEST_ID]
                ,[SD_REQUEST].[RFC_NUMBER]
                ,[SD_REQUEST].[CREATION_DATE_UT]
                ,[SD_REQUEST].[SUBMIT_DATE_UT]
                ,[SD_REQUEST].[END_DATE_UT]
                ,[SD_REQUEST].[MAX_RESOLUTION_DATE_UT]
                ,[SD_REQUEST].[LAST_UPDATE]
                ,[SD_REQUEST].[SUBMITTED_BY]
                ,[SUBMITTED_BY_NAME].[LAST_NAME] AS SUBMITTED_BY_LAST_NAME
                ,[SD_REQUEST].[REQUESTOR_ID]
                ,[REQUESTOR_NAME].[LAST_NAME] AS REQUESTOR_LAST_NAME
                ,[REQUESTOR_LOCATION].[AVAILABLE_FIELD_6] AS REQUESTOR_LOCATION_RH
                ,[SD_REQUEST].[LOCATION_ID]
                ,[SD_REQUEST].[REQUESTOR_PHONE]
                ,[SD_REQUEST].[RECIPIENT_ID]
                ,[RECIPIENT_NAME].[LAST_NAME] AS RECIPIENT_LAST_NAME
                ,[RECIPIENT_LOCATION].[AVAILABLE_FIELD_6] AS RECIPIENT_LOCATION_RH
                ,[SD_REQUEST].[SD_CATALOG_ID]
                ,[SD_CATALOG].[TITLE_FR] AS CATALOG_NAME
                ,[SD_REQUEST].[STATUS_ID]
                ,[SD_STATUS].[STATUS_FR]
                ,[SD_REQUEST].[COMMENT]
                ,[SD_REQUEST].[DESCRIPTION]
                ,[SD_REQUEST].[URGENCY_ID]
                ,[SD_URGENCY].[URGENCY_FR]
                ,[SD_REQUEST].[CI_ID]
                ,[AM_ASSET].[NETWORK_IDENTIFIER] AS CI_NAME
                ,[SD_REQUEST].[OWNER_ID]
                ,[OWNER_NAME].[LAST_NAME] AS OWNER_LAST_NAME
                ,[SD_REQUEST].[OWNING_GROUP_ID]
                ,[AM_GROUP].[GROUP_FR] AS OWNING_GROUP_NAME
                ,[SD_REQUEST].[DEPARTMENT_ID]
                ,[SD_REQUEST].[REQUESTOR_FEEDBACK]
                ,[SD_REQUEST].[ASSET_ID]
                ,[MATERIAL_NETWORK_NAME].[ASSET_TAG]
                ,[SD_REQUEST].[FIRST_CALL_RESOLUTION]
                ,[SD_REQUEST].[SEVERITY_ID]
                ,[SD_REQUEST].[TIME_USED_TO_SOLVE_REQUEST]
                ,[SD_REQUEST].[REQUEST_ORIGIN_ID]
                ,[SD_REQUEST].[DELAY]
                ,[SD_REQUEST].[LAST_GROUP_ID]
                ,[SD_REQUEST].[LAST_DONE_BY_ID]
                ,[SD_REQUEST].[SLA_ID]
                ,[SD_REQUEST].[IMPACT_ID]
                ,[SD_REQUEST].[E_INFRA_COMMENT]
                ,[SD_REQUEST].[IS_MAJOR_INCIDENT]
                ,[SD_REQUEST].[E_TYPE_SUPPORT]
            FROM [EVO_DATA50004].[50004].[SD_REQUEST]
            inner join [EVO_DATA50004].[50004].AM_EMPLOYEE SUBMITTED_BY_NAME on SD_REQUEST.SUBMITTED_BY = SUBMITTED_BY_NAME.EMPLOYEE_ID
            inner join [EVO_DATA50004].[50004].AM_EMPLOYEE REQUESTOR_NAME on SD_REQUEST.REQUESTOR_ID = REQUESTOR_NAME.EMPLOYEE_ID
            inner join [EVO_DATA50004].[50004].AM_EMPLOYEE RECIPIENT_NAME on SD_REQUEST.RECIPIENT_ID = RECIPIENT_NAME.EMPLOYEE_ID
            left join [EVO_DATA50004].[50004].AM_EMPLOYEE OWNER_NAME on SD_REQUEST.OWNER_ID = OWNER_NAME.EMPLOYEE_ID
            left join [EVO_DATA50004].[50004].AM_EMPLOYEE REQUESTOR_LOCATION on SD_REQUEST.REQUESTOR_ID = REQUESTOR_LOCATION.EMPLOYEE_ID
            left join [EVO_DATA50004].[50004].AM_EMPLOYEE RECIPIENT_LOCATION on SD_REQUEST.RECIPIENT_ID = RECIPIENT_LOCATION.EMPLOYEE_ID
            inner join [EVO_DATA50004].[50004].SD_CATALOG on SD_REQUEST.SD_CATALOG_ID = SD_CATALOG.SD_CATALOG_ID
            inner join [EVO_DATA50004].[50004].SD_STATUS on SD_REQUEST.STATUS_ID = SD_STATUS.STATUS_ID
            inner join [EVO_DATA50004].[50004].SD_URGENCY on SD_REQUEST.URGENCY_ID = SD_URGENCY.URGENCY_ID
            left join [EVO_DATA50004].[50004].AM_ASSET on SD_REQUEST.CI_ID = AM_ASSET.ASSET_ID
            left join [EVO_DATA50004].[50004].AM_ASSET MATERIAL_NETWORK_NAME on SD_REQUEST.ASSET_ID = MATERIAL_NETWORK_NAME.ASSET_ID
            left join [EVO_DATA50004].[50004].AM_GROUP on SD_REQUEST.OWNING_GROUP_ID = AM_GROUP.GROUP_ID
            WHERE (SD_REQUEST.REQUEST_ID in (SELECT TOP (1000) [AM_ACTION].[REQUEST_ID]
                                            FROM [EVO_DATA50004].[50004].[AM_ACTION]
                                            WHERE AM_ACTION.END_DATE_UT IS NULL AND
                                                    AM_ACTION.GROUP_ID = 5) AND
                    SD_REQUEST.MAX_RESOLUTION_DATE_UT IS NOT NULL) OR
                    (SD_REQUEST.REQUEST_ID IN (SELECT TOP (1000) [SD_REQUEST].[REQUEST_ID]
                                                FROM [EVO_DATA50004].[50004].[SD_REQUEST]
                                                WHERE  SD_REQUEST.SD_CATALOG_ID = 5535 AND
                                                        SD_REQUEST.RFC_NUMBER IS NOT NULL AND
                                                        SD_REQUEST.STATUS_ID NOT IN (2,7,8,11,15,18,21,25,26,27,28,30,33)))'''
    
    if site_settings.TESTING:
        log.info("using local SD_REQUEST testing solution")
        tickets_local_export = os.path.join(current_working_directory,"db", "SD_REQUEST_testing.csv")
        result: pd.DataFrame = pd.read_csv(tickets_local_export, sep = ";")
        
        return (result, "123")

    async with lock:
        await asyncio.sleep(0)
        log.info("Lock time: {}".format(datetime.now()))
        cache_checker: Union[str, None] = memcached_client.get('SPOT_requests_fetch_status')
        while cache_checker == "loading":
            time.sleep(1)
            cache_checker = memcached_client.get('SPOT_requests_fetch_status')

        cached_result: pd.DataFrame = memcached_client.get('SPOT_requests')
        if cached_result is not None:
            log.info("SQL REQ SD_REQUEST - Read from Cache")
            cached_uuid: str = memcached_client.get("SPOT_requests_uuid")

            return (cached_result, cached_uuid)
                
        else:
            memcached_client.set('SPOT_requests_fetch_status', "loading", site_settings.CACHE_EXPIRE)
            data_uuid = str(uuid.uuid4())

            if site_settings.DEBUG:
                log.info("using local SD_REQUEST debug solution")
                tickets_local_export = os.path.join(current_working_directory,"db", "SD_REQUEST.csv")
                result: pd.DataFrame = pd.read_csv(tickets_local_export, sep = ";")
                
                memcached_client.set('SPOT_requests_fetch_status', "done", site_settings.CACHE_EXPIRE)
                memcached_client.set('SPOT_requests', result, site_settings.CACHE_EXPIRE)
                memcached_client.set('SPOT_requests_uuid', data_uuid, site_settings.CACHE_EXPIRE)
                
                return (result, data_uuid)


            with engine().connect() as connection:
                result: pd.DataFrame = pd.read_sql(requests_sql_query,
                                con = connection)

                log.info("SQL Server request made: SD_REQUEST")

                memcached_client.set('SPOT_requests_fetch_status', "done", site_settings.CACHE_EXPIRE)
                memcached_client.set('SPOT_requests', result, site_settings.CACHE_EXPIRE)
                memcached_client.set('SPOT_requests_uuid', data_uuid, site_settings.CACHE_EXPIRE)
                
                log.info("SD_REQUEST saved to cache - uuid {}".format(data_uuid))

                return (result, data_uuid)
            



async def get_tasks(engine: Callable[[], Engine], memcached_client: Client, lock) -> Tuple[pd.DataFrame, Union[str, None]]:
    ''' Creates a connexion with SPOT SQL server and executes query

    Args:
        engine: the function which returns the sqlalchemy connexion

    Returns:
        df_tickets_operations: dataframe with the results

            The fields in the result:
            * ACTION_ID: id of the action
            * REQUEST_ID: id of the request it relates to
            * RFC_NUMBER: unique number in format (I/D)YYMM1234 of the ticket it relates to
            * ACTION_NUMBER: ???
            * ASSET_ID: ???
            * PARENT_ACTION_ID: ??? 
            * VALIDATOR_ID: ???
            * ACTION_LABEL_FR: name of the action
            * ACTION_TYPE_ID: id of the action type
                **	1	Validation Self Service
                **	2	Logistique
                **	4	Installation Transition
                **	5	Suspension
                **	6	Fin de suspension
                **	7	Prise d'appel
                **	13	Escalade
                **	14	Redirection
                **	15	Réattribution
                **	16	Qualification
                **	17	Traitement
                **	18	Envoi de mail
                **	19	Messagerie instantanée
                **	20	Traitement Operation
                **	21	Traitement Transition
                **	22	Fin du Workflow
                **	23	Etape conditionnelle
                **	24	Nouvelle demande
                **	25	Demande Self Service
                **	26	Mise à jour des erreurs connus
                **	27	Réouverture Incident
                **	28	Clôture anticipée
                **	30	Etape de mise à jour interne
                **	31	Evaluation
                **	32	Validation Operation
                **	33	Validation Transition
                **	34	Clôture Service Desk
                **	35	Etude d'impact
                **	36	Réouverture Demande
                **	37	Clôture Transition
                **	38	Validation Self Service avec Rating
                **	39	Absence
                **	40	Information DSI
                **	41	Appel prestataire externe
                **	42	Résolution par le parent
                **	43	Etape WebService
                **	44	Annulation
                **	45	Envoi en réparation
                **	46	Maintenance préventive
                **	47	Clôture par affectation à une demande
                **	48	Notification
                **	49	Enregistrer une indisponibilité
                **	50	Traitement Automatique
                **	51	Affectation à un incident parent
                **	52	Redirigé vers un niveau inférieur
                **	54	Installation
                **	55	Intervention sur site CNR
                **	56	Intervention sur Site Externe
                **	57	Prise de main à distance
                **	58	Reboot équipement
                **	59	Réinitialisation compte
                **	60	Relance de l'utilisateur
                **	61	Relance vers l'utilisateur
                **	62	Suspension de l'utilisateur
                **	63	Suspension prestataire
                **	64	Vérification droits
                **	65	Information pilote
                **	67	Analyse avant CAB
                **	68	Analyse avant CAB
                **	69	Modification du paramétrage
                **	70	Correction d'application
                **	71	Astreinte - Appel niveau 2
                **	72	Révision de plan de reprise 
                **	73	Test de plan de reprise 
                **	74	Brouillons
                **	75	Mise en production
                **	76	Utilisation d'un plan de reprise
                **	77	Mise à jour par l&#39;utilisateur
                **	78	Mise à jour de l&#39;urgence
                **	79	Mise en production effectuée
                **	80	Affectation à une mise en production
                **	81	Affectation
                **	82	Calcul de la date de résolution maximum
                **	83	Validation Operation avec Authentification
                **	84	Validation Transition avec Authentification
                **	85	Appel sortant utilisateur
                **	86	Appel sortant niveau 2
                **	87	Appel entrant utilisateur
                **	88	Prise de contrôle à distance
                **	89	Télédistribution
                **	90	Appel sur incident de masse
                **	91	Attente d'application externe
                **	92	Installation Operation
                **	93	Erreur connue liée
                **	94	Mise à jour incidents liés à un problème
                **	95	Association de processus
                **	96	Demande d'information
                **	97	Email entrant
                **	98	Suppression du contenu de la demande
                **	99	Annulation d'affectation
                **	100	Tâche projet
                **	101	Point d'avancement
                **	102	Jalon
                **	103	Fin anticipée
                **	104	Validation Self Service avec Authentification
                **	105	Planifier avec détection des collisions
                **	106	Etape conditionnelle assistée
                **	107	Historique des mouvements
                **	108	Information DSI
                **	109	Installation Click2Get
                **	110	Fin de qualification niveau 3
                **	111	Étape REST
            * ACTION_TYPE: name of the action type
            * START_DATE_UT: start date of the action
            * END_DATE_UT: end date of the action
            * EXPECTED_START_DATE_UT: ???
            * CREATION_DATE_UT: creation in the DB of the action
            * EXPECTED_END_DATE_UT: ???
            * LAST_UPDATE: last update of the action
            * PROCESS_STEP_ID: ??? related to workflow ?
            * LOCATION_ID: ???
            * DONE_BY_ID: id of the operator who closed the task
            * DONE_BY_NAME: name of the operator who closed the task
            * GROUP_ID: id of the group of the operator who closed the task
                ** 5	Hotline
                ** 8	SUP_DSI_PILOTAGE
                ** 10	PRO_DSI_Gestion du patrimoine
                ** 19	PRO_DSI_Autres applications .Net
                ** 22	PRO_FCT_MUSTANG
                ** 26	PRO_FCT_SAP CO CONT GESTION
                ** 27	PRO_FCT_RH
                ** 29	PRO_FCT_BATHY
                ** 47	DEV_DSI_Autres
                ** 64	DEV_FCT_OPIUM
                ** 74	DEV_FCT_HYDROMET
                ** 76	INFRA_DSI_RESEAU
                ** 81	INFRA_DSI_TELEPHONIE
                ** 82	INFRA_DSI_PDT
                ** 98	PRO_DSI_SIG
                ** 108	DSI Budget
                ** 110	PRO_DSI_CAODAO
                ** 111	SUP_DSI
                ** 112	CAB
                ** 128	DEV_PIL_HYDROMET
                ** 159	PRO_PIL_ACTIONNAIRES
                ** 182	PRO_PIL_COURRIER
                ** 184	PRO_PIL_CV
                ** 187	PRO_PIL_DEMVEH
                ** 189	PRO_PIL_PALLAS
                ** 204	PRO_PIL_GESTDI
                ** 212	PRO_PIL_HEC-RAS
                ** 233	PRO_PIL_OUTILS ENQUETES - SPHINX
                ** 240	PRO_PIL_PLAN DE GESTION
                ** 261	PRO_PIL_TALREN 4
                ** 268	PRO_PIL_WINFLUID
                ** 270	PRO_PIL_WORKFLOW QUALITE
                ** 277	PRO_DSI_Autres progiciels
                ** 282	PRO_FCT_BATHY_DRA
                ** 284	PRO_FCT_BATHY_DRVA
                ** 312	DEV_DSI_HYDROMET
                ** 317	Tous les utilisateurs
                ** 335	DEV_FCT_CDB
                ** 352	PRO_PIL_MIG
                ** 372	SUP_DSI_DEPLOIEMENT
                ** 377	DEV_FCT_CASSANDRE
                ** 382	SUP_DSI_DRA
                ** 399	DEV_FCT_CEMMII
                ** 440	INFRA_DSI_PACKAGE
                ** 471	DEV_DSI_VS
                ** 500	INFRA_DSI_TELEPHONIE_MOBILE
                ** 527	DSI_FCT_CASTEL
                ** 528	DEV_PIL_CORPO
                ** 532	RESP_Hotline
                ** 544	DEV_DSI_NEPTUNE
                ** 551	DEV_DSI_FUDAA-LSPIV
                ** 552	DEV_FCT_FUDAA-LSPIV
                ** 554	PRO_PIL_PORTAILFORMATION
                ** 557	INFRA_DSIN_CGN
                ** 567	DSIN_APP
                ** 572	DIRECTION_REP_DFCG
                ** 583	DIRECTION_REP_DRS
                ** 587	DIRECTION_REP_DVPMIG
            * GROUP_FR: name of the group of the operator who closed the task
            * CONTACT_ID: ???
            * ELAPSED_TIME: ???
            * STATUS_ID_ON_CREATE: ??? 
            * STATUS_ID_ON_TERMINATE: ???
            * TIME_USED_TO_COMPLETE_ACTION: ??? 
            * ORIGIN_ACTION_ID: id of the action that triggered the creation of the actual action
            * WORKFLOW_VALUE: ???
            * WORKFLOW_ID: id of the workflow, if any
            * EXIT_VALUE: ???
            * PREVIOUS_SIBLING_ID: ???
            * HISTORY_ID: ???
    '''

    from apollo.main import site_settings
    requests_tasks_sql_query: str = '''SELECT TOP (100000) [AM_ACTION].[ACTION_ID]
                                    ,[AM_ACTION].[REQUEST_ID]
                                    ,[SD_REQUEST].[RFC_NUMBER]
                                    ,[AM_ACTION].[ACTION_NUMBER]
                                    ,[AM_ACTION].[ASSET_ID]
                                    ,[AM_ACTION].[PARENT_ACTION_ID]
                                    ,[AM_ACTION].[VALIDATOR_ID]
                                    ,[AM_ACTION].[ACTION_LABEL_FR]
                                    ,[AM_ACTION].[ACTION_TYPE_ID]
                                    ,[AM_ACTION_TYPE].[NAME_FR] AS ACTION_TYPE
                                    ,[AM_ACTION].[START_DATE_UT]
                                    ,[AM_ACTION].[END_DATE_UT]
                                    ,[AM_ACTION].[EXPECTED_START_DATE_UT]
                                    ,[AM_ACTION].[CREATION_DATE_UT]
                                    ,[AM_ACTION].[EXPECTED_END_DATE_UT]
                                    ,[AM_ACTION].[LAST_UPDATE]
                                    ,[AM_ACTION].[DESCRIPTION]
                                    ,[AM_ACTION].[PROCESS_STEP_ID]
                                    ,[AM_ACTION].[LOCATION_ID]
                                    ,[AM_ACTION].[DONE_BY_ID]
                                    ,[AM_EMPLOYEE].[LAST_NAME] AS DONE_BY_NAME
                                    ,[AM_ACTION].[GROUP_ID]
                                    ,[AM_GROUP].[GROUP_FR]
                                    ,[AM_ACTION].[CONTACT_ID]
                                    ,[AM_ACTION].[ELAPSED_TIME]
                                    ,[AM_ACTION].[STATUS_ID_ON_CREATE]
                                    ,[AM_ACTION].[STATUS_ID_ON_TERMINATE]
                                    ,[AM_ACTION].[TIME_USED_TO_COMPLETE_ACTION]
                                    ,[AM_ACTION].[ORIGIN_ACTION_ID]
                                    ,[AM_ACTION].[WORKFLOW_VALUE]
                                    ,[AM_ACTION].[WORKFLOW_ID]
                                    ,[AM_ACTION].[EXIT_VALUE]
                                    ,[AM_ACTION].[PREVIOUS_SIBLING_ID]
                                    ,[AM_ACTION].[HISTORY_ID]
                                FROM [EVO_DATA50004].[50004].[AM_ACTION]
                                left join [EVO_DATA50004].[50004].AM_ACTION_TYPE on AM_ACTION.ACTION_TYPE_ID = AM_ACTION_TYPE.ACTION_TYPE_ID
                                left join [EVO_DATA50004].[50004].AM_GROUP on AM_ACTION.GROUP_ID = AM_GROUP.GROUP_ID
                                left join [EVO_DATA50004].[50004].AM_EMPLOYEE on AM_ACTION.DONE_BY_ID = AM_EMPLOYEE.EMPLOYEE_ID
                                left join [EVO_DATA50004].[50004].SD_REQUEST on AM_ACTION.REQUEST_ID = SD_REQUEST.REQUEST_ID
                                WHERE AM_ACTION.ACTION_TYPE_ID NOT IN (23,107,82) AND
                                        ((AM_ACTION.REQUEST_ID IN (SELECT TOP (1000) [AM_ACTION].[REQUEST_ID]
                                                                FROM [EVO_DATA50004].[50004].[AM_ACTION]
                                                                WHERE AM_ACTION.END_DATE_UT IS NULL AND
                                                                        AM_ACTION.GROUP_ID = 5
                                                                ORDER BY AM_ACTION.REQUEST_ID)) OR
                                        (AM_ACTION.REQUEST_ID IN (SELECT TOP (1000) [SD_REQUEST].[REQUEST_ID]
                                                                    FROM [EVO_DATA50004].[50004].[SD_REQUEST]
                                                                    WHERE  SD_REQUEST.SD_CATALOG_ID = 5535 AND
                                                                            SD_REQUEST.RFC_NUMBER IS NOT NULL AND
                                                                            SD_REQUEST.STATUS_ID NOT IN (2,7,8,11,15,18,21,25,26,27,28,30,33))))'''

    if site_settings.TESTING:
        log.info("using local AM_ACTION testing solution")
        tickets_local_export = os.path.join(current_working_directory,"db", "AM_ACTION_testing.csv")
        result: pd.DataFrame = pd.read_csv(tickets_local_export, sep = ";")
        
        return (result, "123")

    async with lock:
        await asyncio.sleep(0)
        log.info("Lock time: {}".format(datetime.now()))
        cache_checker: Union[str, None] = memcached_client.get('SPOT_requests_operations_fetch_status')
        while cache_checker == "loading":
            time.sleep(1)
            cache_checker = memcached_client.get('SPOT_requests_operations_fetch_status')

        cached_result: pd.DataFrame = memcached_client.get('SPOT_requests_operations')
        if cached_result is not None:
            log.info("SQL AM_ACTION - Read from Cache")
            cached_uuid: str = memcached_client.get("SPOT_requests_operations_uuid")

            return (cached_result, cached_uuid)
            
        else:
            memcached_client.set('SPOT_requests_operations_fetch_status', "loading", site_settings.CACHE_EXPIRE)
            data_uuid = str(uuid.uuid4())

            if site_settings.DEBUG:
                log.info("using local AM_ACTION debug solution")
                tickets_local_export = os.path.join(current_working_directory,"db", "AM_ACTION.csv")
                result: pd.DataFrame = pd.read_csv(tickets_local_export, sep = ";")
                
                memcached_client.set('SPOT_requests_operations_fetch_status', "done", site_settings.CACHE_EXPIRE)
                memcached_client.set('SPOT_requests_operations', result, site_settings.CACHE_EXPIRE)
                memcached_client.set('SPOT_requests_operations_uuid', data_uuid, site_settings.CACHE_EXPIRE)
                
                return (result, data_uuid)

            with engine().connect() as connection:
                result: pd.DataFrame = pd.read_sql(requests_tasks_sql_query,
                                con = connection)

                log.info("SQL Server request made: AM_action")

                memcached_client.set('SPOT_requests_operations_fetch_status', "done", site_settings.CACHE_EXPIRE)
                memcached_client.set('SPOT_requests_operations', result, site_settings.CACHE_EXPIRE)
                memcached_client.set('SPOT_requests_operations_uuid', data_uuid, site_settings.CACHE_EXPIRE)
                
                log.info("SD_REQUEST saved to cache - uuid {}".format(data_uuid))
                return (result, data_uuid)



def get_employee_mouvement_data(engine, memcache):
    from apollo.main import site_settings
    requests_sql_query = '''SELECT TOP (5000) [SD_QUESTION_RESULT].[REQUEST_ID]
                                    ,[SD_REQUEST].[RFC_NUMBER]
                                    ,[SD_REQUEST].[SD_CATALOG_ID]
                                    ,[SD_QUESTION_RESULT].[QUESTION_ID]
                                    ,[SD_QUESTION_RESULT].[RESULT]
                                    ,[SD_QUESTION_RESULT].[RESULT_STRING_FR]
                            FROM [EVO_DATA50004].[50004].[SD_QUESTION_RESULT]
                            inner join [EVO_DATA50004].[50004].SD_REQUEST on SD_QUESTION_RESULT.REQUEST_ID = SD_REQUEST.REQUEST_ID
                            WHERE (SD_QUESTION_RESULT.REQUEST_ID IN (SELECT TOP (1000) [SD_REQUEST].[REQUEST_ID]
                                                                FROM [EVO_DATA50004].[50004].[SD_REQUEST]
                                                                WHERE  SD_REQUEST.SD_CATALOG_ID = 5535 AND
                                                                        SD_REQUEST.RFC_NUMBER IS NOT NULL AND
                                                                        SD_REQUEST.STATUS_ID NOT IN (2,7,8,11,15,18,21,25,26,27,28,30,33))
                                    AND SD_QUESTION_RESULT.QUESTION_ID in (45,46,48,49,50,55,58))'''

    if site_settings.TESTING:
        log.info("using local SD_QUESTIONS_employee_mouvement testing solution")
        tickets_local_export = os.path.join(current_working_directory,"db", "SD_QUESTION_employee_mouvement_testing.csv")
        result = pd.read_csv(tickets_local_export,
                                    sep = ";")
        return result

    # result = memcache.get('SPOT_employee_mouvement')
    # if result:
    #     log.info("SQL REQ SD_QUESTION_employee_mouvement - Read from Cache")
    #     return result

    else:
        if site_settings.DEBUG:
            log.info("using local SD_QUESTIONS_employee_mouvement debug solution")
            tickets_local_export = os.path.join(current_working_directory,"db", "SD_QUESTION_employee_mouvement.csv")
            result = pd.read_csv(tickets_local_export, sep = ";")
            # memcache.set('SPOT_employee_mouvement', result, site_settings.CACHE_EXPIRE)
            return result


        with engine().connect() as connection:
            result = pd.read_sql(requests_sql_query,
                                con = connection)

            log.info("SQL Server request made: SD_QUESTION_employee_mouvement")
            # memcache.set('SPOT_employee_mouvement', result, site_settings.CACHE_EXPIRE)              
            return result