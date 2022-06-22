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


def read_csv_files():
    '''
    Read the customers and products csv files in dataframe
    and return the list of dataframes.

            Parameters:
                    None

            Returns:
                    list of dataframes (list): list of customer and product dataframe
    '''
    
    params = get_params()

    # dataframe for customer csv file
    customer_df = pd.read_csv(str(params["customers_location"]))
    
    # dataframe for product csv file
    product_df = pd.read_csv(str(params["products_location"]))
    
    return [customer_df, product_df]


def separate_column_elements(transaction_raw_df):

    '''
    Read the transactions json files in dataframe
    and return the dataframes.

            Parameters:
                    None

            Returns:
                    list of dataframes (list): list of customer and product dataframe
    '''

    # exploding basket column's data into individual row as it contain list of dictionary
    intermediate_df = transaction_raw_df.explode('basket').reset_index(drop=True)
    
    # applying pd.Series to each row of "basket" column as it contains dictionary, to achieve further atomicity
    temp_df = intermediate_df['basket'].apply(pd.Series)

    # now adding separate columns for each element of temp_df to intermediate_df(main df)
    intermediate_df[["product_id", "price"]] = temp_df

    # dropping the useless basket column (as we now having it's data separately)
    transaction_df = intermediate_df.drop(['basket'], axis=1)

    return transaction_df

def read_transaction_json_files(transaction_dirs):

    '''
    Read the transactions json files in dataframe
    and return the dataframes.

            Parameters:
                    transaction_dirs (list): list of directories containing transactions.json file

            Returns:
                    transaction_df (Pandas: DataFrame): transactions dataframe
    '''

    # list to store all the JSON lines
    transaction_list = []

    # iterating over folders for transactions.json file
    for transaction in transaction_dirs:

        # opening json file
        with open("./input_data/starter/transactions/"+ str(transaction) +"/transactions.json") as f:
            # spliting and storing each line of file into list
            lines = f.read().splitlines()

            # storing for each files data into common list
            transaction_list.extend(lines)

    # converting list into DataFrame
    intermediate_df = pd.DataFrame(transaction_list)

    # giving temperory column name as json_element
    intermediate_df.columns = ['json_element']

    # applying json.loads function on each row, as each row is a json file and normalizing it to dataframe
    transaction_raw_df = pd.json_normalize(intermediate_df['json_element'].apply(json.loads))

    # exploding and separating elements in "basket" column to achieve atomicity
    transaction_df = separate_column_elements(transaction_raw_df)

    return transaction_df


def merge_dataframes(product_df, customer_df, transaction_df):
    
    intermediate_df = transaction_df.merge(product_df[["product_id", "product_category"]], on="product_id", how="left")

    final_df = intermediate_df.merge(customer_df[["customer_id", "loyalty_score"]], on="customer_id", how="left")
    
    return final_df


def main():
    
    # reading the customer and product csv files in dataframe
    customer_df, product_df = read_csv_files()

    # get parameters
    params = get_params()

    # reading transaction directories in a list
    transaction_dirs = os.listdir(str(params["transactions_location"]))

    # reading transactions json file in dataframe
    transaction_df = read_transaction_json_files(transaction_dirs)

    # merge product and customer dataframes to transaction dataframe for product_category and loyalty_score
    final_df = merge_dataframes(product_df, customer_df, transaction_df)

    # print(final_df[final_df['customer_id'] == 'C1'])

if __name__ == "__main__":
    main()
