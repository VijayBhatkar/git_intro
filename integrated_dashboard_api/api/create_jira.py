from integrated_dashboard_api.core.config import config_svca_details
from integrated_dashboard_api.core.config import config_db_details
from integrated_dashboard_api.db.DatabaseSession import DatabaseSession
import json
from typing import Dict, List, Optional
import requests
from pydantic import BaseModel
from requests.auth import HTTPBasicAuth


class Jira(BaseModel):
    workflows: List[Dict]
    project_key: str = "GDDOH"
    labels: list = ["dfs-out", "Merch-Ecomm"]
    requestor: str = "s0s0n5p"
    components: list = ["Supplychain Stores", "Merch Ecomm"]
    summary: str = "DFS Support Ticket summary"
    description: str = "DFS Support Ticket Description"
    type: str = "Support Incident"
    priority: str = "P2"
    alerttype: Optional[str] = "ERT"


class Jiracreation:
    # """This class is to update assignment table dfs.INTEGRATED_DASHBOARD_ASSIGNMENT """

    def jiraissue(self, package: Jira) -> Dict:
        workflow = getattr(package, "workflows")
        project_key = getattr(package, "project_key")
        labels = getattr(package, "labels")
        requestor = getattr(package, "requestor")
        components = getattr(package, "components")
        summary = getattr(package, "summary")
        description = getattr(package, "description")
        type = getattr(package, "type")
        priority = getattr(package, "priority")
        alert_type = getattr(package, "alerttype")

        print(config_svca_details["service_account"])
        print(config_svca_details["password"])
        v_svca = config_svca_details["service_account"]
        v_passwd = config_svca_details["password"]

        print(workflow)

        ERT_ALERT = 1 if alert_type == "ERT" else 0
        FAILURE_ALERT = 1 if alert_type == "FAILED" else 0
        SLAMISS_ALERT = 1 if alert_type == "SLAMISSED" else 0
        print(ERT_ALERT, FAILURE_ALERT, SLAMISS_ALERT)

        key_list = ["name"]
        n = len(package.components)
        res = []
        for idx in range(0, n):
            res.append({key_list[0]: components[idx]})

        # JIRA
        jira_payload = {
            "fields": {
                "project": {"key": project_key},
                "summary": summary,
                "description": description,
                "issuetype": {"name": type},
                "labels": labels,
                "priority": {"name": priority},
                "components": res
                # "creator": {"name": requestor}
            }
        }
        print(jira_payload)

        response = requests.post(
            "https://jira.walmart.com/rest/api/2/issue/",
            json=jira_payload,
            verify=False,
            auth=HTTPBasicAuth(v_svca, v_passwd),
        )

        a = response.content
        a = json.loads(a.decode("utf-8"))
        print(a)
        jkey = "," + a["key"]

        for ele in workflow:
            PARENT_RUN_ID = ele["PARENT_RUN_ID"]
            for JOB_NAME in ele["jobname"]:
                print(PARENT_RUN_ID, JOB_NAME)
                query1 = f"""
                INSERT INTO dfs.INTEGRATED_DASHBOARD_ASSIGNMENT
                (PARENT_RUN_ID,
                JOB_NAME,
                CREATED_USER,
                UPDATED_USER,
                STATUS,
                JIRA,
                ERT_ALERT,
                FAILURE_ALERT,
                SLAMISS_ALERT)
                VALUES('{PARENT_RUN_ID}', '{JOB_NAME}', '{requestor}', '{requestor}'
                , 'WIP','{jkey}', {ERT_ALERT}, {FAILURE_ALERT}, {SLAMISS_ALERT})
                ON DUPLICATE KEY UPDATE
                UPDATED_USER='{requestor}',
                JIRA=concat(coalesce(JIRA,''),'{jkey}'),
                ERT_ALERT={ERT_ALERT},
                FAILURE_ALERT={FAILURE_ALERT},
                SLAMISS_ALERT={SLAMISS_ALERT}
                """
                print(query1)
                with DatabaseSession(**config_db_details) as session:
                    session.execute(query1)
                    session.commit()
                    session.close()

        return {"msg": "success; Jira Ticket: " + jkey}


if __name__ == "__main__":

    print("create Jira ticket")
