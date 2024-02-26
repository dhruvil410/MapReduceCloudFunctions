import requests
import google.oauth2.id_token
import google.auth.transport.requests
import google.auth


def get_gcloud_access_token():
    creds, project = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, 'https://us-central1-dhruvil-dholariya-fall23.cloudfunctions.net/metadata_generation_func')
    return id_token
tf_idf_url = 'https://us-central1-dhruvil-dholariya-fall23.cloudfunctions.net/metadata_generation_func'
data = {'start' : 3006, 'end' : 3010}
headers={'Authorization': 'bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjBhZDFmZWM3ODUwNGY0NDdiYWU2NWJjZjVhZmFlZGI2NWVlYzllODEiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJhenAiOiIzMjU1NTk0MDU1OS5hcHBzLmdvb2dsZXVzZXJjb250ZW50LmNvbSIsImF1ZCI6IjMyNTU1OTQwNTU5LmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwic3ViIjoiMTAwNDM2MzcyMTQ4MTA5MDMyNjEwIiwiaGQiOiJpdS5lZHUiLCJlbWFpbCI6ImRkaG9sYXJpQGl1LmVkdSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJhdF9oYXNoIjoiZVhNM2pSNEFsNV9CUVBiSkhrM1VKQSIsImlhdCI6MTcwMjIyNzYyNCwiZXhwIjoxNzAyMjMxMjI0fQ.VHmb1060AFCwzQtj8xR-0m45kyUVTNTeXGp7nT-oi06taIszvDMv4e74AHNe0DnCpUUO5w03qPyY1EgQ8_Qqb2nCmMSAa65i0JdtynTfsh5B65EZtsSegPtJgIEwiD8JQmqI7-7HeglDB6Z5jG01p2h2ZWNkdej7_RKmkOSRXX8heKGZtCkWFIng3KD8ldwBrO8Xqh5fHjQ0CxTTZCJHlXnYcr7AGralIqHtu6GuYEi-chrdeQKasDPMkKcCuTzvlv5H760EVWN53jCt8DLWBspCn-HONNoWqe9Up6hTE_qjs08fe_hBK7L-gzBBFaxJrU-g59aHHRz2eMfc1WQThg'}
headers={'Authorization': f'bearer {get_gcloud_access_token()}'}

# response = requests.post(tf_idf_url, json=data, headers=headers)

# print(response.text)
# print(response.status_code)