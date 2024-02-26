from flask import Flask, render_template, flash
from forms import SearchnForm, TfidfForm
from threading import Thread
import requests
import google.auth.transport.requests
import google.oauth2.id_token

app = Flask(__name__)
app.config['SECRET_KEY'] = '335666859d90b4c8787b0d587772410d'
is_tf_idf = 0

@app.route("/", methods=['GET', 'POST'])
def home():
    print("TAG: Processing Search Request")
    form = SearchnForm()

    if(form.validate_on_submit()):
        query = form.query.data
        response = call_search_query_func(query)
        if(response.ok):
            return render_template("home.html", form=form, top_10_results = response.json())
        flash('Server is running in some problems, not able search documents for given query!', 'warning')
    return render_template("home.html", form=form)

@app.route("/tfidf", methods=['GET', 'POST'])
def tfidf():
    print("TAG: Processing TF-IDF Request")
    form = TfidfForm()

    global is_tf_idf
    if(is_tf_idf == 1):
        return render_template("tfidf.html", form=form, loading = True)
    if(is_tf_idf == 2):
        flash('TF-IDF Calculated for Given Document IDs', 'success')
        is_tf_idf = 0
        return render_template("tfidf.html", form=form, loading = False)
    if(is_tf_idf == 3):
        flash('Error in Calculating TF-IDF for Given Document IDs', 'warning')
        is_tf_idf = 0
        return render_template("tfidf.html", form=form, loading = False)
    
    if(form.validate_on_submit()):
        start_id = form.start_id.data
        end_id = form.end_id.data
        n_mappers = form.n_mappers.data
        n_reducers = form.n_reducers.data

        if(start_id > end_id):
            flash('End ID can not be less than Start ID', 'warning')
        elif(start_id+30 < end_id):
            flash('Range of documents should be less than 30', 'warning')
        else:
            thread = Thread(target = call_if_idf_func, args = (start_id, end_id, n_mappers, n_reducers))
            is_tf_idf = 1
            thread.start()
            return render_template("tfidf.html", form=form, loading = True)
    return render_template("tfidf.html", form=form)

def call_search_query_func(query):
    # changes
    print("TAG: Calling Search Query Function")

    search_url = 'https://us-central1-dhruvil-dholariya-fall23.cloudfunctions.net/search_query_func'
    data = {'query' : query}
    # headers = {'Authorization': f'bearer {get_gcloud_access_token(search_url)}'}
    headers = {'Authorization': 'bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjBhZDFmZWM3ODUwNGY0NDdiYWU2NWJjZjVhZmFlZGI2NWVlYzllODEiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJhenAiOiIzMjU1NTk0MDU1OS5hcHBzLmdvb2dsZXVzZXJjb250ZW50LmNvbSIsImF1ZCI6IjMyNTU1OTQwNTU5LmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwic3ViIjoiMTAwNDM2MzcyMTQ4MTA5MDMyNjEwIiwiaGQiOiJpdS5lZHUiLCJlbWFpbCI6ImRkaG9sYXJpQGl1LmVkdSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJhdF9oYXNoIjoidUpUSXExTTh3cmhXTmZQa2xieGhMdyIsImlhdCI6MTcwMjI0MDM0OCwiZXhwIjoxNzAyMjQzOTQ4fQ.klcCLKPxi-icIeAxRRirh9xrl3S0OeodgtfAc4LYgHtykjkBGudsUTOQrdtZodqfHpv9dsP2uNYHO-VOEmusiH2t7IKD_mMZdQHpt_szJMfJusvn5mN200K-M6LxkoboepdN0Oq-u4OLn1AvjawReqEDomotB-6wqqmdtrway6Gi7fNPAMpjARQvXfyCz-1o3imzScbSl6YnTpvK0yGxjPHYvawFWBuPdjhu0Dq7Peeft5A-3HxOJ49c8W1VsO0VW93yhcfYRKP9LZDVOpYnOu0gMURtL8uZ9WBkIQjmkt892jQc-GxP-KlzY5x4u_dfyFrkWZA0x1lfkbR6Ado1PA'}

    return requests.post(search_url, json=data, headers=headers)

def call_if_idf_func(start_id, end_id, n_mappers, n_reducers):
    print("TAG: Calling TF-IDF Function")

    tf_idf_url = 'https://us-central1-dhruvil-dholariya-fall23.cloudfunctions.net/tf_idf_func'
    data = {'bucket_name' : 'mr_search', 'start' : start_id, 'end' : end_id, 'n_mapper':n_mappers, 'n_reducer':n_reducers}
    # headers = {'Authorization': f'bearer {get_gcloud_access_token(tf_idf_url)}'}
    headers = {'Authorization': 'bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjBhZDFmZWM3ODUwNGY0NDdiYWU2NWJjZjVhZmFlZGI2NWVlYzllODEiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJhenAiOiIzMjU1NTk0MDU1OS5hcHBzLmdvb2dsZXVzZXJjb250ZW50LmNvbSIsImF1ZCI6IjMyNTU1OTQwNTU5LmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwic3ViIjoiMTAwNDM2MzcyMTQ4MTA5MDMyNjEwIiwiaGQiOiJpdS5lZHUiLCJlbWFpbCI6ImRkaG9sYXJpQGl1LmVkdSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJhdF9oYXNoIjoidUpUSXExTTh3cmhXTmZQa2xieGhMdyIsImlhdCI6MTcwMjI0MDM0OCwiZXhwIjoxNzAyMjQzOTQ4fQ.klcCLKPxi-icIeAxRRirh9xrl3S0OeodgtfAc4LYgHtykjkBGudsUTOQrdtZodqfHpv9dsP2uNYHO-VOEmusiH2t7IKD_mMZdQHpt_szJMfJusvn5mN200K-M6LxkoboepdN0Oq-u4OLn1AvjawReqEDomotB-6wqqmdtrway6Gi7fNPAMpjARQvXfyCz-1o3imzScbSl6YnTpvK0yGxjPHYvawFWBuPdjhu0Dq7Peeft5A-3HxOJ49c8W1VsO0VW93yhcfYRKP9LZDVOpYnOu0gMURtL8uZ9WBkIQjmkt892jQc-GxP-KlzY5x4u_dfyFrkWZA0x1lfkbR6Ado1PA'}

    response =  requests.post(tf_idf_url, json=data, headers=headers)

    global is_tf_idf
    if(response.ok):
        is_tf_idf = 2
    else:
        is_tf_idf = 3

def get_gcloud_access_token(url):
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)
    return id_token

    