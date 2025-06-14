from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, Integer, String, Text

Base = declarative_base()

class DicomIndex(Base):
    __tablename__ = "dicom_index"

    index_id = Column(Integer, primary_key=True, autoincrement=True)
    
    group_name = Column(String)

    file_path = Column(String)
    
    sop_class_uid = Column(String)
    modality = Column(String)
    patient_id = Column(String)
    study_instance_uid = Column(String)
    series_instance_uid = Column(String)
    sop_instance_uid = Column(String)
    
    header_data = Column(Text)
    header_digest = Column(String)
    header_size = Column(BigInteger)
    
    meta_data = Column(Text)
    meta_digest = Column(String)
    meta_size = Column(BigInteger)
    
    pixel_data = Column(Text, nullable=True)
    pixel_digest = Column(String, nullable=True)
    pixel_size = Column(BigInteger, nullable=True)