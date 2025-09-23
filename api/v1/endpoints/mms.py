from database.MongoDBConnector import MongoDBConnector
from schemas.mms import AnnotationPayload, BadMeasurementPayload

from fastapi import APIRouter, Query

router = APIRouter(prefix="/mms")

mongo = MongoDBConnector(mode="remote")

@router.post(
    path="/upsert_annotation",
    status_code=202
)
async def upsert_annotation(payload: AnnotationPayload):

    m_date  = payload.m_date
    mobile  = payload.mobile
    notes   = payload.notes
    annotations = payload.annotations

    record = {
        '_id'               : f"{mobile}{m_date}",
        'measurement_date'  : m_date,
        'mobile'            : mobile,
        'annotations'       : [
            {
                'start' : i.start,
                'end'   : i.end,
                'description' : i.description
            }\
            for i in annotations
        ],
        'notes'             : notes
    }

    await mongo.upsert_documents_hashed([record], coll_name='traceannotations')

    return {"status": "OK"}

@router.get(
    path="/get_annotation",
    status_code=200
)
async def get_annotation(
        mobile: str = Query(alias="mobile"),
        m_date: str = Query(alias="measurement_date")
):

    annotation = await mongo.get_all_documents(
        coll_name="traceannotations",
        query={
            "_id" : f"{mobile}{m_date}"
        }
    )

    return {"exists" : True, **annotation[0]} if annotation else {"exists": False}

@router.post(
    path="/insert_bad_measurement",
    status_code=202
)
async def insert_bad_measurement(payload: BadMeasurementPayload):

    m_date = payload.m_date
    mobile = payload.mobile

    record = {
        '_id' : f"{mobile}{m_date}",
        'measurement_date' : m_date,
        'mobile' : mobile,
    }

    await mongo.upsert_documents_hashed([record], coll_name='bad_measurements')

    return {"status": "OK"}

@router.post(
    path="/remove_bad_measurement",
    status_code=202
)
async def remove_bad_measurement(
        mobile: str = Query(alias="mobile"),
        m_date: str = Query(alias="measurement_date")
):

    await mongo.delete_document(
        coll_name="bad_measurements",
        query={
            "_id" : f"{mobile}{m_date}"
        }
    )

    return {"status": "OK"}
