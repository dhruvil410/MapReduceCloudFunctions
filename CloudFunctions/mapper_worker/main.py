from google.cloud import storage

def mapper_worker_func(request):
    try:
        request_json = request.get_json(force=True, silent=True, cache=True)  

        # Parse the input data
        bucket_name = None
        output_dir = None
        filename = None
        n_reducer = None
        mapper_str = None
        combiner_str = None

        if(request_json and 'bucket_name' in request_json and 'output_dir' in request_json and 'filename' in request_json 
           and 'n_reducer' in request_json and 'mapper_str' in request_json):
            bucket_name = request_json['bucket_name']
            output_dir = request_json['output_dir']
            filename = request_json['filename']
            n_reducer = request_json['n_reducer']
            mapper_str = request_json['mapper_str']

            if('combiner_str' in request_json):
                combiner_str = request_json['combiner_str']

        elif(request.args and 'bucket_name' in request.args and 'output_dir' in request.args and 'filename' in request.args 
             and 'n_reducer' in request.args and 'mapper_str' in request.args):
            bucket_name = request.args.get('bucket_name')
            output_dir = request.args.get('output_dir')
            filename = request.args.get('filename')
            n_reducer = request.args.get('n_reducer')
            mapper_str = request.args.get('mapper_str')

            if('combiner_str' in request.args):
                combiner_str = request.args.get('combiner_str')
        else:
            return "Please pass bucket_name, output_dir, filename, n_reducer, and mapper_str parameters in HTTP request!", 422

        print("TAG: Mapper: ", filename)
        
        mapper = evaluate_str_function(mapper_str, 'mapper')

        if(combiner_str):
            combiner = evaluate_str_function(combiner_str, 'combiner')

        base_filename = filename.split('/')[-1]
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        data = read_file(bucket, filename)
        mapper_output = {}

        # Call the mapper for each line
        for line in data.splitlines():
            one_mapper_output = mapper(base_filename, line)
            merge_mapper_output(mapper_output, one_mapper_output)

        # Call the combiner for each key if combiner is available
        if(combiner_str):
            for key, value in mapper_output.items():
                mapper_output[key] = combiner(key, value)
        
        # Distribute output per reducer
        distributed_intermediate_output = []
        for _ in range(n_reducer):
            distributed_intermediate_output.append({})

        for key, value in mapper_output.items():
            reducer_index = hash_func(key)%n_reducer
            distributed_intermediate_output[reducer_index][key] = value
        
        for index, intermediate_output in enumerate(distributed_intermediate_output):
            store_intermediate_output(bucket, intermediate_output, output_dir, index, base_filename)
        
        return "Mapper Worker Request Completed!", 200

    except Exception as e:
        print("TAG: ", e)
        return 'Mapper Worker Request is not completed', 500
    
def store_intermediate_output(bucket, intermediate_output, output_dir, index, base_filename):
    if(output_dir[-1] != '/'):
        output_dir += '/'
    
    filename = f"{output_dir}{index}/{base_filename}"
    content = convert_intermediate_output_to_string(intermediate_output)
    
    blob = bucket.blob(filename)
    generation_match_precondition = 0

    if(blob.exists()):
        blob.reload()
        generation_match_precondition = blob.generation

    blob.upload_from_string(content, if_generation_match = generation_match_precondition)


def convert_intermediate_output_to_string(intermediate_output):
    result_list = list(map(lambda x: x[0] + "\n" + str(x[1]), intermediate_output.items()))
    return "\n".join(result_list)

def merge_mapper_output(mapper_output, one_mapper_output):
    for key, value in one_mapper_output:
        if(key not in mapper_output):
            mapper_output[key] = []
        
        mapper_output[key].append(value)

def read_file(bucket, filename):
    blob = bucket.blob(filename)
    return blob.download_as_text()

def evaluate_str_function(func_str, func_name):
    functions = {}
    exec(func_str, functions)
    return functions[func_name]

def hash_func(key):
    key = str(key)
    sum = 0
    for ch in key:
        sum += ord(ch)
    return sum