import inspect
import requests
from google.cloud import storage
import json
import ast
import google.auth.transport.requests
import google.oauth2.id_token

def tf_idf_func(request):
    try:
        request_json = request.get_json(force=True, silent=True, cache=True)  

        bucket_name = None
        start = None
        end = None
        n_mapper = None
        n_reducer = None

        # Parse the input data
        if(request_json and 'bucket_name' in request_json and 'start' in request_json and 'end' in request_json and
           'n_mapper' in request_json and 'n_reducer' in request_json):
            bucket_name = request_json['bucket_name']
            start = int(request_json['start'])
            end = int(request_json['end'])
            n_mapper = int(request_json['n_mapper'])
            n_reducer = int(request_json['n_reducer'])
        elif(request.args and 'bucket_name' in request.args and 'start' in request.args and 'end' in request.args and
             'n_mapper' in request.args and 'n_reducer' in request.args):
            bucket_name = request.args.get('bucket_name')
            start = int(request.args.get('start'))
            end = int(request.args.get('end'))
            n_mapper = int(request.args.get('n_mapper'))
            n_reducer = int(request.args.get('n_reducer'))
        else:
            return "Please pass bucket_name, start, end, n_mapper, and n_reducer parameters in HTTP request!", 422
        
        # Download Dataset
        # chages
        download_dataset_url = 'https://us-central1-dhruvil-dholariya-fall23.cloudfunctions.net/download_dataset_func'
        download_dataset_params = {'bucket_name' : bucket_name, 'start' : start, 'end' : end}
        headers = {'Authorization': f'bearer {get_gcloud_access_token(download_dataset_url)}'}
        
        response = requests.post(download_dataset_url, json=download_dataset_params, headers=headers)

        if(not response.ok):
            return 'Download Dataset Request is not completed', 500
        print("TAG: Files Downloaded!")

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # Parameters for MapReduce Job
        input_dir = get_input_dir_name(bucket)
        output_dir = get_output_dir_name(bucket)
        mapper_str = inspect.getsource(mapper)
        combiner_str = inspect.getsource(combiner)
        reducer_str = inspect.getsource(reducer)
        
        # Start MapReduce Job
        # chages
        master_url = 'https://us-central1-dhruvil-dholariya-fall23.cloudfunctions.net/master_func'
        master_params = {'bucket_name' : bucket_name, 'n_mapper' : n_mapper, 'n_reducer' : n_reducer, 'mapper_str' : mapper_str,
                'combiner_str' : combiner_str, 'reducer_str' : reducer_str, 'input_dir' : input_dir, 'output_dir' : output_dir}
        headers = {'Authorization': f'bearer {get_gcloud_access_token(master_url)}'}

        response = requests.post(master_url, json=master_params, headers=headers)

        if(not response.ok):
            return 'Map Reduce Request is not completed', 500
        print("TAG: MapReduce Work Done!")
        

        # Merge currently calculated TF-IDF with exisiting

        tf_idf_metadata_filename = "tf_idf_metadata.json"
        tf_idf_metadata = json.loads(load_file(bucket, tf_idf_metadata_filename))
        # Converted to set so that time complexity of checking id is present or not become O(1)
        tf_idf_metadata['doc_ids'] = set(tf_idf_metadata['doc_ids'])

        tf_idf_data_path = "tf_idf_data/"
        tf_idf_data_filenames = list_files(storage_client, bucket_name, tf_idf_data_path)
        tf_idf_data_list = load_tf_idf_data(tf_idf_data_filenames, bucket)

        reducer_output_files = list_files(storage_client, bucket_name, output_dir)

        for output_file in reducer_output_files:
            reducer_output_list = load_file(bucket, output_file).splitlines()
            length = len(reducer_output_list)

            for i in range(0, length, 2):
                key = reducer_output_list[i] 
                value = ast.literal_eval(reducer_output_list[i+1])

                merge_with_tf_idf(tf_idf_metadata, tf_idf_data_list, key, value)

        tf_idf_metadata['doc_ids'] = list(tf_idf_metadata['doc_ids'])
        store_tf_idf_file(bucket, json.dumps(tf_idf_metadata), tf_idf_metadata_filename)
        for tf_idf_filename, tf_idf_data in zip(tf_idf_data_filenames, tf_idf_data_list):
            store_tf_idf_file(bucket, convert_tf_idf_data_to_string(tf_idf_data), tf_idf_filename)

        # Deletes input and output files
        delete_input_files(bucket, list_files(storage_client, bucket_name, input_dir))
        delete_input_files(bucket, reducer_output_files)

        return "TF-IDF Request Completed!", 200
    
    except Exception as e:
        print("TAG: ", e)
        return 'TF-IDF Request is not completed', 500
    

def mapper(key, value):
    import re
    output = []
    key = key[:-4]
    value = re.sub(r'[^\w\s]', '', value).lower()

    for word in value.split(' '):
        if(word == ''):
            continue
        output.append([word, [key, 1]])
    return output

def combiner(key, value):
    return [[value[0][0], len(value)]]

def reducer(key, value):
    return value

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

def load_file(bucket, filename):
    blob = bucket.blob(filename)
    return blob.download_as_text()

def load_tf_idf_data(tf_idf_data_filenames, bucket):
    tf_idf_data_list = []
    for filename in tf_idf_data_filenames:
        tf_idf_data = {}

        content = load_file(bucket, filename)
        for content_line in content.splitlines():
            pair = content_line.split("\t")
            tf_idf_data[pair[0]] = {'count' : int(pair[1]), 'value' : ast.literal_eval(pair[2])}

        tf_idf_data_list.append(tf_idf_data)
    
    return tf_idf_data_list

def list_files(storage_client, bucket_name, prefix):
    if(prefix[-1] != '/'):
        prefix += '/'
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter='/')

    filename_list = []
    for blob in blobs:
        filename_list.append(blob.name)
    return filename_list

def merge_with_tf_idf(tf_idf_metadata, tf_idf_data_list, key, value):
    if(key not in tf_idf_metadata['terms']):
        file_index = hash(key) % (len(tf_idf_data_list))
        tf_idf_metadata['terms'][key] = file_index        
        tf_idf_data_list[file_index][key] = {'count' : 0, 'value' : {}}

    file_index = tf_idf_metadata['terms'][key]
    cur_tf = tf_idf_data_list[file_index][key]

    for doc_id, freq in value:
        if(doc_id not in tf_idf_metadata['doc_ids']):
            tf_idf_metadata['doc_ids'].add(doc_id)
            tf_idf_metadata['doc_count'] += 1

        if(doc_id not in cur_tf['value']):
            cur_tf['count'] += 1
        
        cur_tf['value'][doc_id] = freq

def convert_tf_idf_data_to_string(tf_idf_data):
    data_list = []
    for term, term_dict in tf_idf_data.items():
        count = term_dict['count']
        value = json.dumps(term_dict['value'])
        data_str = f"{term}\t{count}\t{value}"
        data_list.append(data_str)

    return "\n".join(data_list)

def store_tf_idf_file(bucket, content, tf_idf_filename):
    blob = bucket.blob(tf_idf_filename)
    generation_match_precondition = 0

    if(blob.exists()):
        blob.reload()
        generation_match_precondition = blob.generation

    blob.upload_from_string(content, if_generation_match = generation_match_precondition)

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













