import json 

def remove_duplicates(filename : str) -> None:

    "Convert: List[ Dict[ str, Any ] ] => Dict[ str, Dict[ str, Any ] ]"

    assert isinstance(filename, str)

    dataframe = open(filename, mode = "r", encoding = "utf-8").read()

    if (dataframe[0] == "{"):
        return

    if (dataframe[0] != "["):
        raise Exception("Parsing Error! Expected dictionary as outer container!")
    
    dataframe = json.loads(dataframe)

    dataframe = { item["Title"] : item for item in dataframe }

    json.dump(dataframe, open(filename, mode = "w", encoding = "utf-8"), indent = 4)

if (__name__ == "__main__"):

    dataframe = "dataset/taigi.json"

    remove_duplicates(dataframe)