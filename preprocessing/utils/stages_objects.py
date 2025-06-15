import datetime
from typing import Dict, Any, List, Mapping

from preprocessing.mongo_db.mongodb import MongodbObject
from preprocessing.utils.attachments_extraction import PageRegions
from preprocessing.utils.page_regions import  TableRegion


class GeneralInfo(MongodbObject):

    def __init__(self, ELI: str, document_year: int,
                    status: str, announcementDate: str, volume: int,
                    address: str, displayAddress: str, promulgation: str,
                    pos: int, publisher: str, changeDate: str, textHTML: bool,
                    textPDF: bool, title: str, type: str, filename_of_pdf: str,
                    s3_pdf_path: str):
        self.ELI = ELI
        self.document_year = document_year
        self.status = status
        self.announcementDate = announcementDate
        self.volume = volume
        self.address = address
        self.displayAddress = displayAddress
        self.promulgation = promulgation
        self.pos = pos
        self.publisher = publisher
        self.changeDate = changeDate
        self.textHTML = textHTML
        self.textPDF = textPDF
        self.title = title
        self.type = type
        self.filename_of_pdf = filename_of_pdf
        self.s3_pdf_path = s3_pdf_path


    def to_dict(self):
        return {
            "ELI": self.ELI,
            "document_year": self.document_year,
            "status": self.status,
            "announcementDate": self.announcementDate,
            "volume": self.volume,
            "address": self.address,
            "displayAddress": self.displayAddress,
            "promulgation": self.promulgation,
            "pos": self.pos,
            "publisher": self.publisher,
            "changeDate": self.changeDate,
            "textHTML": self.textHTML,
            "textPDF": self.textPDF,
            "title": self.title,
            "type": self.type,
            "filename_of_pdf": self.filename_of_pdf,
            "s3_pdf_path": self.s3_pdf_path
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            ELI=dict_object['ELI'],
            document_year=dict_object['document_year'],
            status=dict_object['status'],
            announcementDate=dict_object['announcementDate'],
            volume=dict_object['volume'],
            address=dict_object['address'],
            displayAddress=dict_object['displayAddress'],
            promulgation=dict_object['promulgation'],
            pos=dict_object['pos'],
            publisher=dict_object['publisher'],
            changeDate=dict_object['changeDate'],
            textHTML=dict_object['textHTML'],
            textPDF=dict_object['textPDF'],
            title=dict_object['title'],
            type=dict_object['type'],
            filename_of_pdf=dict_object['filename_of_pdf'],
            s3_pdf_path=dict_object['s3_pdf_path']
        )



class EstablishLegalActSegmentsBoundariesResult(MongodbObject):

    def __init__(self, general_info: GeneralInfo, invoke_id: str, page_regions: PageRegions,
                 expires_at: datetime.datetime):
        self.general_info = general_info
        self.invoke_id = invoke_id
        self.page_regions = page_regions
        self.expires_at = expires_at

    def to_dict(self):
        return {
            "general_info": GeneralInfo.to_dict(self.general_info),
            "invoke_id": self.invoke_id,
            "page_regions": PageRegions.to_dict(self.page_regions)
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            general_info=GeneralInfo.from_dict(dict_object['general_info']),
            invoke_id=dict_object['invoke_id'],
            page_regions=PageRegions.from_dict(dict_object['page_regions']),
            expires_at=dict_object['expires_at']
        )






class EstablishLegalWithTablesResult(MongodbObject):

    def to_dict(self):
        return {
            "general_info": GeneralInfo.to_dict(self.general_info),
            "invoke_id": self.invoke_id,
            "page_regions": PageRegions.to_dict(self.page_regions),
            "tables_regions": self.tables_regions
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            general_info=GeneralInfo.from_dict(dict_object['general_info']),
            invoke_id=dict_object['invoke_id'],
            page_regions=PageRegions.from_dict(dict_object['page_regions']),
            tables_regions=[TableRegion.from_dict(table_region_dict) for table_region_dict in dict_object['tables_regions']],
            expires_at=dict_object['expires_at']
        )

    def __init__(self, general_info: GeneralInfo, invoke_id: str, page_regions: PageRegions,
                 tables_regions: List[TableRegion], expires_at: datetime.datetime):
        self.general_info = general_info
        self.invoke_id = invoke_id
        self.page_regions = page_regions
        self.tables_regions = tables_regions
