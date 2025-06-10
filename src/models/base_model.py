from pydantic import BaseModel


class Metadata(BaseModel):
    column_name: str
    description: str
    data_type: str