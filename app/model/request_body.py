from pydantic import BaseModel


class My_request(BaseModel):
    data_path:str
    output_path:str