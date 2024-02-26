from google.cloud import storage
import ast

def reducer_worker_func(request):
    try:
        request_json = request.get_json(force=True, silent=True, cache=True)  

        bucket_name = None
        output_dir = None
        reducer_index = None
        reducer_str = None

        # Parse the input data
        if(request_json and 'bucket_name' in request_json and 'output_dir' in request_json and 'reducer_index' in request_json and
           'reducer_str' in request_json):
            bucket_name = request_json['bucket_name']
            output_dir = request_json['output_dir']
            reducer_index = request_json['reducer_index']
            reducer_str = request_json['reducer_str']
        elif(request.args and 'bucket_name' in request.args and 'output_dir' in request.args and 'reducer_index' in request.args and
             'reducer_str' in request.args):
            bucket_name = request.args.get('bucket_name')
            output_dir = request.args.get('output_dir')
            reducer_index = request.args.get('reducer_index')
            reducer_str = request.args.get('reducer_str')
        else:
            return "Please pass bucket_name, output_dir, reduce_index, and reducer_str parameters in HTTP request!", 422
    
        print("TAG: Reducer: ", reducer_index)

        reducer = evaluate_str_function(reducer_str, 'reducer')
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        input_files = list_input_files(bucket_name, output_dir, reducer_index)
        
        reducer_input = {}
        for filename in input_files:
            data = read_file(bucket, filename)
            merge_data_in_reducer_input(reducer_input, data)

        reducer_output = {}
        for key, value in reducer_input.items():
            reducer_output[key] = reducer(key, value)
        
        store_reducer_output(bucket, reducer_output, output_dir, reducer_index)

        delete_input_files(bucket, input_files)

        return "Reducer Worker Request Completed!", 200

    except Exception as e:
        print("TAG: ", e)
        return 'Reducer Worker Request is not completed', 500
    
def merge_data_in_reducer_input(reducer_input, data):
    lines = data.splitlines()
    length = len(lines)
    
    for i in range(0, length, 2):
        key = lines[i] 
        value = lines[i+1]

        if(key not in reducer_input):
            reducer_input[key] = []
        reducer_input[key].extend(ast.literal_eval(value))


def list_input_files(bucket_name, output_dir, index):
    storage_client = storage.Client()

    if(output_dir[-1] != '/'):
        output_dir += '/'
    
    prefix = output_dir + str(index) + "/"
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter='/')

    filename_list = []
    for blob in blobs:
        filename_list.append(blob.name)
    
    return filename_list

def read_file(bucket, filename):
    blob = bucket.blob(filename)
    return blob.download_as_text()

def evaluate_str_function(func_str, func_name):
    functions = {}
    exec(func_str, functions)
    return functions[func_name]

def store_reducer_output(bucket, reducer_output, output_dir, index):
    if(output_dir[-1] != '/'):
        output_dir += '/'
    
    filename = f"{output_dir}reducer_{index}"
    content = convert_reducer_output_to_string(reducer_output)
    
    blob = bucket.blob(filename)
    generation_match_precondition = 0

    if(blob.exists()):
        blob.reload()
        generation_match_precondition = blob.generation

    blob.upload_from_string(content, if_generation_match = generation_match_precondition)


def convert_reducer_output_to_string(reducer_output):
    result_list = list(map(lambda x: x[0] + "\n" + str(x[1]), reducer_output.items()))
    return "\n".join(result_list)

def delete_input_files(bucket, input_files):
    for filename in input_files:
        blob = bucket.blob(filename)
        blob.reload()
        generation_match_precondition = blob.generation
        blob.delete(if_generation_match=generation_match_precondition)
