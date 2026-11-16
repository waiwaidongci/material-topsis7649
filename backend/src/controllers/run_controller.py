from src.schemas.run import RunOut
from src.services import run_service as service
def index(db):return service.list_runs(db)
def create(db,data):return service.create_run(db,data)

def detail(db,id):
    item=service.get_run(db,id)
    out=RunOut.model_validate(item)
    if item.status!='completed':out.result={}
    return out
