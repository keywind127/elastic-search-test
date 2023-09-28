import json

def reformat_json(filename : str) -> None:

    "Convert: Dict[ str, Dict[ str, Any ] ] => List[ Dict[ str, Any ] ]"

    assert isinstance(filename, str)

    dataframe = open(filename, mode = "r", encoding = "utf-8").read()

    if (dataframe[0] == "["):
        return

    if (dataframe[0] != "{"):
        raise Exception("Parsing Error! Expected dictionary as outer container!")
    
    dataframe = list(json.loads(dataframe).values())

    json.dump(dataframe, open(filename, mode = "w", encoding = "utf-8"), indent = 4)

if (__name__ == "__main__"):

    dataframe = "dataset/taigi.json"

    reformat_json(dataframe)