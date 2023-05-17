import Extractor as E, Transformer as T, Loader as L
import pickle

# Main script
extractor = E.Extractor()
transformer = T.Transformer()
loader = L.Loader()



def initial_etl():
    skip_extraction = True
    if not skip_extraction:
        extracted_data = extractor.initial_extraction()
        print(extracted_data.keys())
        print(extracted_data["source1"].keys())
        print(extracted_data["source2"].keys())
        print(extracted_data["source3"].keys())
    
        with open('saved_extracted_data.pkl', 'wb') as f:
            pickle.dump(extracted_data, f)
    else:
            
        with open('saved_extracted_data.pkl', 'rb') as f:
            extracted_data = pickle.load(f)
    transformed_data = transformer.transform_initial_data(extracted_data)
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