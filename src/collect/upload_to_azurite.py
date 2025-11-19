from pathlib import Path
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

AZURITE_CONN_STR = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)

def upload_bronze_file(dataset_code: str):
    local_dir = Path("data/lake/bronze") / dataset_code.lower()
    latest_file = sorted(local_dir.glob("*.json"))[-1]
    print(f"Uploading {latest_file} to Azurite...")

    blob_service_client = BlobServiceClient.from_connection_string(AZURITE_CONN_STR)

    # container name i vlefshëm: vetem a-z, 0-9, -
    safe_code = dataset_code.lower().replace("_", "-")
    container_name = f"bronze-{safe_code}"
    print(f"Using container name: {container_name}")

    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.create_container()
        print(f"Created container: {container_name}")
    except ResourceExistsError:
        print(f"Container {container_name} already exists.")
    # mos kap Exception generic, se fsheh gabime reale

    blob_name = latest_file.name
    with latest_file.open("rb") as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)

    print(f"Uploaded to container {container_name} as blob {blob_name}.")

if __name__ == "__main__":
    upload_bronze_file("DEMO_PJAN")