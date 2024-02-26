import inspect
import requests
from google.cloud import storage
import json
import re
import heapq
import google.auth.transport.requests
import google.oauth2.id_token

def search_query_func(request):
    try:
        request_json = request.get_json(force=True, silent=True, cache=True)  

        query = None
        
        # Parse the input data
        if(request_json and 'query' in request_json ):
            query = request_json['query']
        elif(request.args and 'query' in request.args):
            query = request.args.get('query')
        else:
            return "Please pass query parameters in HTTP request!", 422
        
        # Prepare the input
        bucket_name = "mr_search"
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        tf_idf_metadata_filename = "tf_idf_metadata.json"
        tf_idf_metadata = json.loads(load_file(bucket, tf_idf_metadata_filename))

        tf_idf_data_path = "tf_idf_data/"
        tf_idf_data_filenames = list_files(storage_client, bucket_name, tf_idf_data_path)

        word_count = prepare_word_count(query)
        for word in word_count:
            prepare_input_file_for_word(bucket, word, tf_idf_metadata, tf_idf_data_filenames)
        
        print("TAG: Input Prepared!")

        # Parameters for MapReduce Job
        n_mapper = 3
        n_reducer = 5
        input_dir = get_input_dir_name(bucket)
        output_dir = get_output_dir_name(bucket)
        mapper_str = return_mapper_str(word_count, tf_idf_metadata)
        reducer_str = inspect.getsource(reducer)
        
        # Start MapReduce Job
        # chages
        master_url = 'https://us-central1-dhruvil-dholariya-fall23.cloudfunctions.net/master_func'
        master_params = {'bucket_name' : bucket_name, 'n_mapper' : n_mapper, 'n_reducer' : n_reducer, 'mapper_str' : mapper_str,
                'reducer_str' : reducer_str, 'input_dir' : input_dir, 'output_dir' : output_dir}
        headers = {'Authorization': f'bearer {get_gcloud_access_token(master_url)}'}
        
        response = requests.post(master_url, json=master_params, headers=headers)

        if(not response.ok):
            return 'Map Reduce Request is not completed', 500
        print("TAG: MapReduce Work Done!")

        # Process the search results
        reducer_output_files = list_files(storage_client, bucket_name, output_dir)
        top_10_doc_id = []
        for output_file in reducer_output_files:
            reducer_output_list = load_file(bucket, output_file).splitlines()
            length = len(reducer_output_list)

            for i in range(0, length, 2):
                key = reducer_output_list[i] 
                value = reducer_output_list[i+1]

                if(len(top_10_doc_id) < 10):
                    heapq.heappush(top_10_doc_id, (value, key))
                elif(value > top_10_doc_id[0][0]):
                    heapq.heappop(top_10_doc_id)
                    heapq.heappush(top_10_doc_id, (value, key))
        
        top_10_doc_id.sort(reverse=True)
        gutenberg_metadata_filename = "gutenberg_metadata.json"
        gutenberg_metadata = json.loads(load_file(bucket, gutenberg_metadata_filename))

        top_10_results = []
        for probability, doc_id in top_10_doc_id:
            doc_id = str(doc_id)
            print("TAG: ", doc_id, probability)
            top_10_results.append(gutenberg_metadata[doc_id])
        
        # Deletes input and output files
        delete_input_files(bucket, list_files(storage_client, bucket_name, input_dir))
        delete_input_files(bucket, reducer_output_files)
        
        return top_10_results, 200
    
    except Exception as e:
        print("TAG: ", e)
        return 'Search Query request is not completed', 500

def return_mapper_str(word_count, tf_idf_metadata):
    mapper_str = f"""
def mapper(key, value):
    import json
    import math
    output = []

    word_count = {word_count}
    n_doc = {tf_idf_metadata['doc_count']}

    splited_value = value.split("\t")
    word = splited_value[0]
    df = int(splited_value[1])
    tf_dict = json.loads(splited_value[2])

    tf_query = word_count[word]
    if_idf_query = (1 + math.log10(tf_query)) * math.log10(n_doc/df)
    
    for doc_id, tf in tf_dict.items():
        output.append([doc_id, [if_idf_query, 1 + math.log10(tf)]])
    return output
"""
    return mapper_str

def reducer(key, value):
    dot_product = 0
    normalize_sum_query = 0
    normalize_sum_doc = 0

    for tf_idf in value:
        dot_product += (tf_idf[0] * tf_idf[1])
        normalize_sum_query += tf_idf[0] ** 2
        normalize_sum_doc += tf_idf[1] ** 2
    normalize_sum_query = normalize_sum_query ** 0.5
    normalize_sum_doc = normalize_sum_doc ** 0.5
    if(normalize_sum_doc == 0.0 or normalize_sum_query == 0):
        return 0
    return dot_product / (normalize_sum_query * normalize_sum_doc)

def get_input_dir_name(bucket):
    dir_name_list = ["input_dir", "input_dir1", "input_dir2", "input_dir3", "input_dir4", "input_dir5"]

    for dir_name in dir_name_list:
        blob = bucket.blob(dir_name)

        if(not blob.exists()):
            return dir_name

def get_output_dir_name(bucket):
    dir_name_list = ["output_dir", "output_dir1", "output_dir2", "output_dir3", "output_dir4", "output_dir5"]

    for dir_name in dir_name_list:
        blob = bucket.blob(dir_name)

        if(not blob.exists()):
            return dir_name

def prepare_word_count(query):
    query = re.sub(r'[^\w\s]', '', query).lower()
    word_count = {}

    for word in query.split(' '):
        if(word == ''):
            continue

        if(word not in word_count):
            word_count[word] = 0
        word_count[word] += 1
    
    return word_count

def load_file(bucket, filename):
    blob = bucket.blob(filename)
    return blob.download_as_text()

def list_files(storage_client, bucket_name, prefix):
    if(prefix[-1] != '/'):
        prefix += '/'
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter='/')

    filename_list = []
    for blob in blobs:
        filename_list.append(blob.name)
    return filename_list

def prepare_input_file_for_word(bucket, word, tf_idf_metadata, tf_idf_data_filenames):
    if(word in tf_idf_metadata['terms']):
        content = load_file(bucket, tf_idf_data_filenames[tf_idf_metadata['terms'][word]])

        word_with_tab = word + '\t'
        for line in content.splitlines():
            if(line.startswith(word_with_tab)):
                blob = bucket.blob("input_dir/" + word)
                generation_match_precondition = 0

                if(blob.exists()):
                    blob.reload()
                    generation_match_precondition = blob.generation

                blob.upload_from_string(line, if_generation_match = generation_match_precondition)
                break

def delete_input_files(bucket, input_files):
    for filename in input_files:
        blob = bucket.blob(filename)
        blob.reload()
        generation_match_precondition = blob.generation
        blob.delete(if_generation_match=generation_match_precondition)

def get_gcloud_access_token(url):
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)
    return id_token