from pydantic import BaseModel, Field

from typing import List

class AnnotationField(BaseModel):

    start       : int
    end         : int
    description : str

class AnnotationPayload(BaseModel):

    # id          : str = Field(validation_alias="_id")
    m_date      : str = Field(validation_alias="measurement_date")
    annotations : List[AnnotationField] = Field(validation_alias="annotations")
    notes       : str = Field(validation_alias="notes")
    mobile      : str = Field(validation_alias="mobile")

class BadMeasurementPayload(BaseModel):

    m_date : str = Field(validation_alias="measurement_date")
    mobile : str = Field(validation_alias="mobile")
