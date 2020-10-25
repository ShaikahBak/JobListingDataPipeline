import json

filenames = ['BusinessAnalyst', 'DataAnalyst', 'DataEngineer', 'DataScientist']

original_keys = [['FIELD1', 'index', 'Job Title', 'Salary Estimate'],
                 ['Job Description'],
                 ['Rating', 'Company Name', 'Location', 'Headquarters',
                  'Size', 'Founded', 'Type of ownership', 'Industry', 'Sector',
                  'Revenue', 'Competitors', 'Easy Apply']]
newer_keys = [['field1', 'index_', 'job_title', 'salary_estimate'],
              ['job_description'],
              ['rating', 'company_name', 'location', 'headquarters',
               'size_', 'founded', 'type_of_ownership', 'industry', 'sector',
               'revenue', 'competitors', 'easy_apply']]

job_id = 0

def create_json(path, lst):
    for item in lst:
        open(f"data/job_listings_json/{path}", "a").write(
            json.dumps(item, separators=(",", ":"))
        )

def add_to_dictionary(part_, ob, orig_keys, new_keys):
    dict_ = {'job_id': job_id}
    for x in range(len(orig_keys)):
        if orig_keys[x] in ob[i]:
            val = ob[i].pop(orig_keys[x])
            if isinstance(val, str):
                val = val.replace("'", "")
                val = val.replace("\n", " ")
            dict_[new_keys[x]] = val

    part_.append(dict_)

for file in filenames:
    obj = json.load(open(f"data/job_listings_json/{file}.json"))

    # storing job listing info in three different parts
    # to reduce the size of objects loaded into Redshift
    parts = [[], [], []]

    for i in range(len(obj)):
        # separate objects into three parts
        for y in range(len(parts)):
            add_to_dictionary(parts[y], obj, original_keys[y], newer_keys[y])

        job_id += 1


    for y in range(len(parts)):
        n = y+1
        create_json(f"part{n}/{file}_part{n}.json", parts[y])