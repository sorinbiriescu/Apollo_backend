from pydantic import BaseModel
from typing import Optional, List, Dict
from pandas import DataFrame

class UserModel(BaseModel):
    username: str
    fullname: str
    email: Optional[str] = None
    password: str
    disabled: int
    class Config:
        orm_mode = True

class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None

class DataRequestModel(BaseModel):
    kpi_list: list
    filter_list: Optional[list] = []

class RequestClassificationChangeModel(BaseModel):
    AP_SD_REQUEST_ID: int
    Type: str

class RequestDataModel(BaseModel):
    filter_user: Optional[str] = None
    filter_start_date: Optional[str] = None
    filter_start_enddate: Optional[str] = None


class DatatableItemResponseModel(BaseModel):
    SPOT: str
    Statut: str
    Beneficiaire: str
    Location: str
    Priorite: str
    CI: str
    Description: str
    C_RDV_DATE: str
    C_RDV_STATE: str
    AP_AM_DONE_BY_OPERATOR_NAME: str
    Score: int
    C_POINTS_JUSTIFICATION:  str
    AP_INTERVENTION_TYPE: int
    Inter_Type: str
    C_TICKET_TYPE: int
    C_TICKET_TYPE_STRING_FR: str
    AP_SD_REQUEST_ID: str

# ===========================
class SuspendedMailNotRecontactedColumnsModel(BaseModel):
    col_order: int
    name: str
    id: str

class SuspendedMailNotRecontactedItemModel(BaseModel):
    AP_SD_RFC_NUMBER: str
    AP_TYPE_FR: str
    AP_SD_RECIPIENT_LOCATION_RH: str
    AP_AM_DONE_BY_OPERATOR_NAME: str

class SuspendedMailNotRecontactedResponseModel(BaseModel):
    columns: List[SuspendedMailNotRecontactedColumnsModel]
    data: List[SuspendedMailNotRecontactedItemModel]

# ===========================

class TicketsByExpirationColumnsModel(BaseModel):
    col_order: int
    name: str
    id: str

class TicketsByExpirationItemModel(BaseModel):
    AP_SD_RFC_NUMBER: str
    AP_TYPE_FR: str
    AP_SD_RECIPIENT_LOCATION_RH: str
    AP_AM_DONE_BY_OPERATOR_NAME: str
    AP_SD_MAX_RESOLUTION_DATE: str

class TicketsByExpirationResponseModel(BaseModel):
    columns: List[TicketsByExpirationColumnsModel]
    data: List[TicketsByExpirationItemModel]

# ===========================

class TotalTicketsByTechColumnsModel(BaseModel):
    col_order: int
    name: str
    col_id: str

class TotalTicketsByTechItemModel(BaseModel):
    tech_name: str
    total_tickets: int
    score: int

class TotalTicketsByTechResponseModel(BaseModel):
    columns: List[TotalTicketsByTechColumnsModel]
    data: List[TotalTicketsByTechItemModel]