"""Upload generated inventory / review / CRM files to Azure Blob Storage.

Layout created in container `retail-landing`:
    inventory/inventory_snapshot_YYYY-MM-DD.csv
    reviews/reviews_YYYY-MM.jsonl
    crm/salesforce_case_export.csv        (fallback if you skip Salesforce)

    set AZURE_STORAGE_CONNECTION_STRING in .env, then:
    python upload_to_blob.py
    python upload_to_blob.py --only inventory --latest 1   # drip-feed one new day
"""
import argparse
import glob
import os

from azure.storage.blob import BlobServiceClient

import config as C

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER", "retail-landing")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["inventory", "reviews", "crm"])
    ap.add_argument("--latest", type=int, default=0, help="upload only the N newest files per folder")
    args = ap.parse_args()

    svc = BlobServiceClient.from_connection_string(os.environ["AZURE_STORAGE_CONNECTION_STRING"])
    cont = svc.get_container_client(CONTAINER)
    if not cont.exists():
        cont.create_container()

    for folder in ["inventory", "reviews", "crm"]:
        if args.only and folder != args.only:
            continue
        files = sorted(glob.glob(os.path.join(C.OUTPUT_DIR, "blob", folder, "*")))
        if args.latest:
            files = files[-args.latest:]
        for path in files:
            name = f"{folder}/{os.path.basename(path)}"
            with open(path, "rb") as f:
                cont.upload_blob(name, f, overwrite=True)
            print(f"  uploaded {name}")
    print("Done.")


if __name__ == "__main__":
    main()
