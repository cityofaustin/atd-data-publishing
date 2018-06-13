"""
Helper methods to work with PostgREST.
"""
import pdb

import requests


class Postgrest(object):
    """
    Class to interact with PostgREST.
    """

    def __init__(self, base_url, auth=None):

        self.auth = auth
        self.base_url = base_url

        headers = {
            "Content-Type": "text/csv",
            "Prefer": "return=representation",  # return entire record json in response
        }

        if self.auth:
            headers["Authorization"] = f"Bearer {self.auth}"

    def select(self, query_string, limit=None):
        url = f"{self.base_url}?{query_string}"
        return self._query("SELECT", url, limit=limit)

    def insert(self, data=None):
        res = requests.post(self.base_url, headers=headers, json=data)

    def update(self, query_string, data=None):
        url = f"{self.base_url}?{query_string}"
        res = requests.patch(url, headers=headers, json=data)

    def upsert(self, data=None):
        return self._query("UPSERT", self.base_url, data=data)

    def delete(self, query_string, data=None):
        url = f"{self.base_url}?{query_string}"
        return self._query("DELETE", url)

    def _query(self, method, url, data=None, limit=None):
        """
        This method is dangerous! It is possible to delete and modify records
        en masse. Read the PostgREST docs.
        """
        headers = {
            "Content-Type": "text/csv",
            "Prefer": "return=representation",  # return entire record json in response
        }

        if method.upper() == "INSERT":
            res = requests.post(url, headers=headers, json=data)

        elif method.upper() == "UPDATE":
            res = requests.patch(url, headers=headers, json=data)

        elif method.upper() == "UPSERT":
            headers["Prefer"] += ", resolution=merge-duplicates"
            res = requests.patch(url, headers=headers, json=data)

        elif method.upper() == "DELETE":
            res = requests.delete(url, headers=headers)

        elif method.upper() == "SELECT":
            """Offset pagination for SELECT requests"""
            records = []

            while True:
                query_url = f"{url}&offset={len(records)}"

                res = requests.get(query_url, headers=headers)

                res.raise_for_status()

                if res.json():
                    records += res.json()

                    if limit:
                        if len(records) >= limit:
                            return records[0:limit]
                else:
                    return records

        else:
            raise Exception("Unknown method requested.")

        res.raise_for_status()
        return res.json()
