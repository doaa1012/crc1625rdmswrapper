from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from fastapi.responses import JSONResponse
from MatInfClient import MatInfWebApiClient
import os
import requests

app = FastAPI(title="CRC1625rdmswrapper")

# ---------- Request Models ----------

class ApiKeyInput(BaseModel):
    api_key: str

class SQLRequest(ApiKeyInput):
    sql: str

class FilteredObjectsRequest(ApiKeyInput):
    associated_typenames: List[str]
    sample_typename: str
    start_date: str
    end_date: str
    strict: Optional[bool] = False

class ElementFilterRequest(ApiKeyInput):
    object_ids: List[int]
    element_criteria: Dict[str, Any]

class SummaryRequest(ApiKeyInput):
    sample_typename: str
    start_date: str
    end_date: str
    include_associated: Optional[bool] = True
    include_properties: Optional[bool] = True
    include_composition: Optional[bool] = True
    include_linked_properties: Optional[bool] = True
    property_names: Optional[List[str]] = []

class ProcessDataRequest(ApiKeyInput):
    associated_typenames: List[str]
    sample_typename: str
    start_date: str
    end_date: str
    element_criteria: Dict[str, Any]
    strict: Optional[bool] = True

# ---------- Helper to extend the client for file downloading ----------

def download_file(base_url: str, relative_path: str, save_dir: str = "results/downloaded_files") -> Optional[str]:
    if not relative_path:
        return None
    os.makedirs(save_dir, exist_ok=True)
    file_url = base_url.rstrip("/") + relative_path
    filename = os.path.basename(relative_path)
    save_path = os.path.join(save_dir, filename)

    try:
        response = requests.get(file_url)
        if response.status_code == 200:
            with open(save_path, "wb") as f:
                f.write(response.content)
            return save_path
    except Exception as e:
        print(f"Failed to download {file_url}: {e}")
    return None

# ---------- Endpoints ----------

@app.post("/execute/")
def execute_sql(req: SQLRequest):
    client = MatInfWebApiClient("https://crc1625.mdi.ruhr-uni-bochum.de", req.api_key)
    result = client.execute(req.sql)
    return JSONResponse(content=result)

@app.post("/get_filtered_objects/")
def get_filtered_objects(req: FilteredObjectsRequest):
    client = MatInfWebApiClient("https://crc1625.mdi.ruhr-uni-bochum.de", req.api_key)
    df, link_map, ids = client.get_filtered_objects(
        req.associated_typenames, req.sample_typename, req.start_date, req.end_date, req.strict
    )
    return {
        "data": df.to_dict(orient="records"),
        "link_map": link_map,
        "object_ids": ids
    }

@app.post("/filter_by_elements/")
def filter_by_elements(req: ElementFilterRequest):
    client = MatInfWebApiClient("https://crc1625.mdi.ruhr-uni-bochum.de", req.api_key)
    df, ids = client.filter_samples_by_elements(req.object_ids, req.element_criteria)
    return {
        "data": df.to_dict(orient="records"),
        "filtered_sample_ids": ids
    }

@app.post("/process/")
def process_data(req: ProcessDataRequest):
    base_url = "https://crc1625.mdi.ruhr-uni-bochum.de"
    client = MatInfWebApiClient(base_url, req.api_key)
    
    df = client.process_data(
        associated_typenames=req.associated_typenames,
        sample_typename=req.sample_typename,
        start_date=req.start_date,
        end_date=req.end_date,
        element_criteria=req.element_criteria,
        strict=req.strict
    )

    # Download linked files
    df["local_filepath"] = df["linked_objectfilepath"].apply(
        lambda path: download_file(base_url, path) if path else None
    )

    return {
        "data": df.to_dict(orient="records"),
        "downloaded_files": df["local_filepath"].dropna().tolist()
    }

@app.post("/summary/")
def get_summary(req: SummaryRequest):
    client = MatInfWebApiClient("https://crc1625.mdi.ruhr-uni-bochum.de", req.api_key)
    summary = client.get_summary(
        sample_typename=req.sample_typename,
        start_date=req.start_date,
        end_date=req.end_date,
        include_associated=req.include_associated,
        include_properties=req.include_properties,
        include_composition=req.include_composition,
        include_linked_properties=req.include_linked_properties,
        property_names=req.property_names
    )
    return JSONResponse(content=summary)
