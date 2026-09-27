"""Load mock support cases into a free Salesforce Developer Edition org.

One-time Salesforce setup (Setup > Object Manager > Case > Fields & Relationships > New):
    Legacy_Case_Id__c      Text(20), Unique, External ID
    Legacy_Customer_Id__c  Text(20), External ID
    Opened_At__c           Date/Time
    Resolved_At__c         Date/Time
    CSAT_Score__c          Number(1,0)
(Why custom date fields? CreatedDate/ClosedDate are system audit fields and
can't be set on insert without extra org permissions. Real migrations hit
exactly this problem — worth mentioning in interviews.)

Then put SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN in .env and run:
    python load_salesforce.py
Skipping Salesforce? Use output/blob/crm/salesforce_case_export.csv instead
(the guide covers both paths).
"""
import csv
import os

from simple_salesforce import Salesforce

import config as C

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def main():
    sf = Salesforce(username=os.environ["SF_USERNAME"],
                    password=os.environ["SF_PASSWORD"],
                    security_token=os.environ["SF_SECURITY_TOKEN"],
                    domain=os.environ.get("SF_DOMAIN", "login"))
    with open(os.path.join(C.OUTPUT_DIR, "salesforce", "Case.csv"), encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for k in list(r):
            if r[k] == "":
                r[k] = None
        if r["CSAT_Score__c"] is not None:
            r["CSAT_Score__c"] = int(r["CSAT_Score__c"])
    # upsert on the external id => re-running the script never creates duplicates
    res = sf.bulk.Case.upsert(rows, "Legacy_Case_Id__c", batch_size=2000, use_serial=False)
    ok = sum(1 for x in res if x.get("success"))
    print(f"Upserted {ok}/{len(rows)} cases.")
    bad = [x for x in res if not x.get("success")][:5]
    for b in bad:
        print("  error:", b.get("errors"))


if __name__ == "__main__":
    main()
