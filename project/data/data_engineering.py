import Extractor as E, Transformer as T, Loader as L


# Main script
extractor = E.Extractor()
transformer = T.Transformer()
loader = L.Loader()



def initial_etl():
    extracted_data = extractor.initial_extraction()
    print(extracted_data.keys())
    print(extracted_data["source1"].keys())
    print(extracted_data["source2"].keys())
    print(extracted_data["source3"].keys())
    #transformed_data = transformer.transform_data(extracted_data)
    #loader.load_data(transformed_data)


def incremental_etl():
    extracted_data = extractor.incremental_extraction()
    print(extracted_data.keys())
    print(extracted_data["source1"].keys())
    print(extracted_data["source2"].keys())
    print(extracted_data["source3"].keys())

    #transformed_data = transformer.transform_data(extracted_data)
    #loader.load_data(transformed_data)


#incremental_etl()
initial_etl()