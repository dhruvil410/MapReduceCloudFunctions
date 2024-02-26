import concurrent.futures
from google.cloud import storage
import requests
import google.auth.transport.requests
import google.oauth2.id_token

def master_func(request):
    try:
        request_json = request.get_json(force=True, silent=True, cache=True)  

        bucket_name = None
        n_mapper = None
        n_reducer = None
        input_dir = None
        output_dir = None
        mapper_str = None
        reducer_str = None
        combiner_str = None

        # Parse the input data
        if(request_json and 'bucket_name' in request_json and 'n_mapper' in request_json and 'n_reducer' in request_json and 
            'input_dir' in request_json and 'output_dir' in request_json and 'mapper_str' in request_json and 
            'reducer_str' in request_json):
            bucket_name = request_json['bucket_name']
            n_mapper = int(request_json['n_mapper'])
            n_reducer = int(request_json['n_reducer'])
            input_dir = request_json['input_dir']
            output_dir = request_json['output_dir']
            mapper_str = request_json['mapper_str']
            reducer_str = request_json['reducer_str']

            if('combiner_str' in request_json):
                combiner_str = request_json['combiner_str']
        elif(request.args and 'bucket_name' in request.args and 'n_mapper' in request.args and 'n_reducer' in request.args and
            'input_dir' in request.args and 'output_dir' in request.args and 'mapper_str' in request.args and 
            'reducer_function' in request.args):
            bucket_name = request.args.get('bucket_name')
            n_mapper = int(request.args.get('n_mapper'))
            n_reducer = int(request.args.get('n_reducer'))
            input_dir = request.args.get('input_dir')
            output_dir = request.args.get('output_dir')
            mapper_str = request.args.get('mapper_str')
            reducer_str = request.args.get('reducer_str')
            if('combiner_str' in request.args):
                combiner_str = request.args.get('combiner_str')
        else:
            return "Please pass bucket_name, n_mapper, n_reducer, input_dir, output_dir, mapper_str, and reducer_str parameters in HTTP request!", 422
    
        print("TAG: ", bucket_name, n_mapper, n_reducer, input_dir, output_dir)

        # List input files
        filename_list = list_input_files(bucket_name, input_dir)
        n_files = len(filename_list)

        # Start Mapper Worker
        with concurrent.futures.ThreadPoolExecutor(n_mapper) as executor:
            mapper_responses = executor.map(mapper_worker, [bucket_name] * n_files, [output_dir] * n_files, filename_list, 
                    [n_reducer] * n_files, [mapper_str] * n_files, [combiner_str] * n_files)

        for response in mapper_responses:
            if(not response.ok):
                return 'Map Reduce Request is not completed', 500
        print("TAG: Mapper Task Completed!")

        # Start Reducer Worker
        with concurrent.futures.ThreadPoolExecutor(n_reducer) as executor:
            reducer_responses = executor.map(reducer_worker, [bucket_name] * n_reducer, [output_dir] * n_reducer, 
                                             [i for i in range(n_reducer)], [reducer_str] * n_reducer)

        for response in reducer_responses:
            if(not response.ok):
                return 'Map Reduce Request is not completed', 500
        print("TAG: Reducer Task Completed!")

        return "Map Reduce Work Done!", 200

    except Exception as e:
        print("TAG: ", e)
        return 'Map Reduce Request is not completed', 500

def mapper_worker(bucket_name, output_dir, filename, n_reducer, mapper_str, combiner_str):
    # Changes
    mapper_worker_url = 'https://us-central1-dhruvil-dholariya-fall23.cloudfunctions.net/mapper_worker_func'
    mapper_worker_params = {'bucket_name' : bucket_name, 'output_dir' : output_dir, 'filename' : filename, 'n_reducer' : n_reducer,
                            'mapper_str' : mapper_str}
    headers = {'Authorization': f'bearer {get_gcloud_access_token(mapper_worker_url)}'}
    if(combiner_str):
        mapper_worker_params['combiner_str'] = combiner_str

    for _ in range(5):
        response = requests.post(mapper_worker_url, json=mapper_worker_params, headers=headers)
        if(response.ok):
            return response

    return response

def reducer_worker(bucket_name, output_dir, reducer_index, reducer_str):
    # Changes
    reducer_worker_url = 'https://us-central1-dhruvil-dholariya-fall23.cloudfunctions.net/reducer_worker_func'
    reducer_worker_params = {'bucket_name' : bucket_name, 'output_dir' : output_dir, 'reducer_index' : reducer_index, 
                            'reducer_str' : reducer_str}
    headers = {'Authorization': f'bearer {get_gcloud_access_token(reducer_worker_url)}'}
    for _ in range(5):
        response = requests.post(reducer_worker_url, json=reducer_worker_params, headers=headers)
        if(response.ok):
            return response

    return response

def list_input_files(bucket_name, input_dir):
    storage_client = storage.Client()

    if(input_dir[-1] != '/'):
        input_dir += '/'
    blobs = storage_client.list_blobs(bucket_name, prefix=input_dir, delimiter='/')

    filename_list = []
    for blob in blobs:
        filename_list.append(blob.name)
    
    return filename_list


def get_gcloud_access_token(url):
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)
    return id_token