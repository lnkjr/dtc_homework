import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path

@task(retries=5)
def dl_from_web(dataset_url: str) -> pd.DataFrame:
    df = pd.read_csv(dataset_url)
    return df

@task(retries=3)
def store_local(df: pd.DataFrame, dataset_file: str)-> Path:
    local_path = Path(f"data/fhv/{dataset_file}.csv")
    df.to_csv(local_path)
    return local_path

@task(retries=3)
def upload_to_gcs(path: Path)->None:
    gcs_block = GcsBucket.load("zoom-bucket")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path.as_posix()
    )


@flow()
def etl_sub_flow(month,year):
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month}.csv.gz" #need to customize, only valid if month is 2 digits, single digit month must have "0" first before the number
    dataset_file = f'fhv_tripdata_{year}-{month}' #need to customize, only valid if month is 2 digits, single digit month must have "0" first before the number
    df = dl_from_web(dataset_url)
    path = store_local(df,dataset_file)
    upload_to_gcs(path)

@flow()
def etl_main_flow(months: list,year: int):
    for month in months:            #maybe add if statement where month is a single digit or double digits
        etl_sub_flow(month, year)
    

if __name__ == '__main__':
    months = [10,11,12] 
    year = 2019
    etl_main_flow(months, year)