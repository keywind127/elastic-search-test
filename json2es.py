from remove_duplicates import remove_duplicates
from format_json import reformat_json
import pandas, utils, sys, os

if (__name__ == "__main__"):


    # >> CONSTANTS 

    COLUMN_NAMES = ( "Title", "Type", "Author", "Date", "Body", "Url", "Age", "Source", "Language" )

    MAPPINGS = {
        "properties" : {
            COLUMN_NAMES[0] : { "type" : "text", "analyzer" : "standard", "tokenizer" : "smartcn" },
            COLUMN_NAMES[1] : { "type" : "text", "analyzer" : "keyword" },
            COLUMN_NAMES[2] : { "type" : "text", "analyzer" : "standard", "tokenizer" : "smartcn" },
            COLUMN_NAMES[3] : { "type" : "text", "analyzer" : "standard", "tokenizer" : "smartcn" },
            COLUMN_NAMES[4] : { "type" : "text", "analyzer" : "standard", "tokenizer" : "smartcn" },
            COLUMN_NAMES[5] : { "type" : "text", "analyzer" : "standard", "tokenizer" : "smartcn" },
            COLUMN_NAMES[6] : { "type" : "text", "analyzer" : "keyword" },
            COLUMN_NAMES[7] : { "type" : "text", "analyzer" : "keyword" },
            COLUMN_NAMES[8] : { "type" : "text", "analyzer" : "keyword" }
        }
    }

    TEST_CREATE = 0

    TEST_INSERT = 1

    TEST_DBREAD = 2

    TEST_REMOVE = 3

    # << CONSTANTS


    # >> PARAMS

    elastic_search_uri = "http://localhost:9200"

    index_name         = "literature"

    folder_of_interest = "dataset"
    
    test = TEST_DBREAD

    # << PARAMS


    folder_of_interest = os.path.join(os.path.dirname(__file__), folder_of_interest)

    files_in_folder = list(map(lambda x : os.path.join(folder_of_interest, x), os.listdir(folder_of_interest)))

    es = utils.Elasticsearch(elastic_search_uri, request_timeout = 30)

    if (test == TEST_CREATE):

        utils.create_index(es, index_name = index_name, mappings = MAPPINGS)

    if (test == TEST_INSERT):

        for json_file_name in files_in_folder:

            if (json_file_name[-5:] != ".json"):
                continue 

            remove_duplicates(json_file_name)

            reformat_json(json_file_name)

            dataframe = pandas.read_json(json_file_name)

            num_items = len(dataframe)

            source = os.path.splitext(os.path.basename(json_file_name))[0]

            print(f"SOURCE: [ {source} ]")

            column_names = set(dataframe.columns)

            for row_idx, row_data in dataframe.iterrows():

                sys.stdout.write(f"\rPROGRESS: [ {(row_idx + 1)/num_items*100:.2f}% ]")

                document = {
                    "Title"  : "",
                    "Type"   : "",
                    "Author" : "",
                    "Date"   : "",
                    "Body"   : "",
                    "Url"    : "",
                    "Age"    : "",
                    "Source" : source
                }

                for column_name in COLUMN_NAMES:
                    if (column_names.__contains__(column_name)):
                        document[column_name] = row_data[column_name]

                for key, val in list(document.items()):

                    if ((isinstance(val, str)) and (val == "None")):
                        val = ""

                    if (isinstance(val, list)):
                        val = "".join(val)

                    if (pandas.isna(val)):
                        document[key] = ""

                utils.insert_document(es, index_name = index_name, document = document)

                sys.stdout.flush()

            print(" => complete\n")

    if (test == TEST_DBREAD):

        print(utils.search_documents(es, index_name, query_dict = { "match_all" : {} }))

        #utils.count_documents(es, index_name, query_dict = { "match_all" : {} })

        query_dict = {
            "term" : {
                "Source" : "taigi"
            }
            # "match_all" : {}
        }

        print("Num Docs: {}".format(utils.count_documents(es, index_name, query_dict = query_dict)))

    if (test == TEST_REMOVE):

        utils.delete_index(es, index_name)
