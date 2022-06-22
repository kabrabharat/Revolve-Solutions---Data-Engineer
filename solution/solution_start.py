import argparse
import pandas as pd
import os
import json


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())



def main():
    params = get_params()

    ## some testings
     
    customer_df = pd.read_csv(str(params["customers_location"]))
    #print(customer_df.head(5))

    transaction_dirs = os.listdir(str(params["transactions_location"]))

    # print(os.listdir(str(params["transactions_location"])))
    
    for inner_dir in transaction_dirs:
        # print(str(params["transactions_location"]) + str(inner_dir) + "/transactions.json")
        transactions_file = open(str(params["transactions_location"]) + str(inner_dir) + "/transactions.json", 'r')
        for line in transactions_file:
            transactions_data = json.loads(line)
            print(transactions_data)
            break
        break
        

    # transactions_df = pd.read_json(str(params["transactions_location"]))
    # print(transactions_df)


    

if __name__ == "__main__":
    main()
