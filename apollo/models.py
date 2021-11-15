from sqlalchemy import (Column, ForeignKey, Integer, String)
from sqlalchemy.orm import relationship

from apollo.database import Base


class UserDB(Base):
    __tablename__ = "user"
    
    id = Column(Integer, primary_key=True)
    username = Column(String(100), unique=True)
    fullname = Column(String(100))
    password = Column(String(100))
    email = Column(String(100))
    disabled = Column(Integer)


class AP_SD_INTERVENTION_CLASSIFICATION_MODEL(Base):
    __tablename__ = 'AP_SD_INTERVENTION_CLASSIFICATION'

    id = Column(Integer, primary_key=True, nullable=False)
    AP_REQUEST_ID = Column(Integer, unique=True)
    AP_RFC_NUMBER = Column(String(100), unique=True, nullable=False)
    AP_INTERVENTION_TYPE = Column(Integer, ForeignKey(
        'AP_SD_INTERVENTION_TYPE.id'), nullable=False)

    intervention_rel = relationship("AP_SD_INTERVENTION_TYPE_MODEL")


class AP_SD_INTERVENTION_TYPE_MODEL(Base):
    __tablename__ = 'AP_SD_INTERVENTION_TYPE'

    id = Column(Integer, primary_key=True, nullable=False)
    AP_TYPE = Column(String(100), unique=True, nullable=False)
    AP_TYPE_FR = Column(String(100), unique=True, nullable=False)
    AP_Description = Column(String(100))
    AP_Description_FR = Column(String(100))
