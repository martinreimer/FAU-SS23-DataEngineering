import Extractor as E, Transformer as T, Loader as L


# Main script
extractor = E.Extractor()
transformer = T.Transformer()
loader = L.Loader()



def initial_etl():
    extracted_data = extractor.initial_extraction()
    #transformed_data = transformer.transform_data(extracted_data)
    #loader.load_data(transformed_data)


def incremental_etl():
    extracted_data = extractor.extract_data()
    #transformed_data = transformer.transform_data(extracted_data)
    #loader.load_data(transformed_data)


initial_etl()