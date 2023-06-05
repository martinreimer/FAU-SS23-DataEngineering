import Extractor as E, Transformer as T, Loader as L
import pickle
import sys

# Initialize ETL
extractor = E.Extractor()
transformer = T.Transformer()
loader = L.Loader()


def initial_etl():
    print("-"*60)
    print("Initial ETL started")
    print()
    skip_extraction = False
    if not skip_extraction:
        extracted_data = extractor.initial_extraction()
    
        with open('saved_extracted_data.pkl', 'wb') as f:
            pickle.dump(extracted_data, f)
    else:
        with open('saved_extracted_data.pkl', 'rb') as f:
            extracted_data = pickle.load(f)
    transformed_data = transformer.transform_data(extracted_data)
    loader.load_initial_data(transformed_data)
    print()
    print("-"*60)
    print("Initial ETL finished")



def incremental_etl():
    print("-"*60)
    print("Incremental ETL started")
    print()
    extracted_data = extractor.incremental_extraction()
    transformed_data = transformer.transform_data(extracted_data)
    loader.load_incremental_data(transformed_data)
    print()
    print("Incremental ETL finished")
    print("-"*60)


#make py file runnable in ssh
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python data_engineering.py [initial_etl|incremental_etl]")
    else:
        command = sys.argv[1]
        if command == "initial_etl":
            initial_etl()
        elif command == "incremental_etl":
            incremental_etl()
        else:
            print("Invalid command. Available options: initial_etl, incremental_etl")