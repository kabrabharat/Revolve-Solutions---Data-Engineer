import argparse
from typing import final
import pandas as pd
import os
import json
from datetime import datetime
import re


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())


def read_csv_files() -> list:
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


def separate_column_elements(transaction_raw_df) -> pd.DataFrame:
    '''
    Separate the list or dictionary present/kept in 'basket' column
    and return whole dataframe

            Parameters:
                    transaction_raw_df (Pandas: DataFrame): raw transactions dataframe having data clubbed in 'basket' column

            Returns:
                    transaction_df (Pandas: DataFrame): Preprocessed transactions dataframe
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

def read_transaction_json_files(transaction_dirs) -> pd.DataFrame:
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

def sorted_alphanumeric_Ids(customers) -> list:
    '''
    Sort the AlphaNumeric Customer Ids(String)

            Parameters:
                    customers (list): list of customer_ids

            Returns:
                    sorted customers (list): list of sorted customer_ids
    '''
    # anonymous function to convert numbers(string) to int after split
    convert = lambda text: int(text) if text.isdigit() else text
    # anonymous function to split numbers from each id
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(customers, key = alphanum_key)


def generate_json(df, name):

    '''
    Create JSON files using weekly data

            Parameters:
                    df (Pandas: DataFrame): final weekly dataframe

            Returns:
                    Nothing
    '''

    # list of unique customers
    customers = list(df['customer_id'].unique())

    #sort customers
    customers = sorted_alphanumeric_Ids(customers)
    
    output_dict = {}

    for customer in customers:
        output_dict[customer] = {}
        
        #initializing purchase_count with 0
        output_dict[customer]['purchase_count'] = 0

        # iteration over rows where customer Ids match
        for idx, row in df[df['customer_id'] == customer].iterrows():

            #storing data to dictionary as required
            output_dict[customer]['loyalty_score'] = row['loyalty_score']
            if 'product_id' in output_dict[customer]:
                output_dict[customer]['product_id'].append(row['product_id'])
                output_dict[customer]['purchase_count'] += 1
            else:
                output_dict[customer]['product_id'] = []
            if 'product_category' in output_dict[customer]:
                output_dict[customer]['product_category'].append(row['product_category'])
            else:
                output_dict[customer]['product_category'] = []
    
    # to get output location parameter
    params = get_params()

    # checking if the given directory already present in the system (if not creating it)
    if os.path.exists(params['output_location']) == False:
        os.makedirs(params['output_location'], exist_ok=True)
        
    # dumping dictionary to JSON file with naming convension --> Week_<<sunday_date>>.json
    with open('./output_data/outputs/Week_'+str(name).split(' ')[0]+'.json', 'w', encoding='utf-8') as f:
        json.dump(output_dict, f, ensure_ascii=False, indent=4)
    

def segregate_weekly(final_df):

    '''
    Segregate the DataFrame Week-wise.

            Parameters:
                    final_df (Pandas: DataFrame): Final DataFrame with whole data

            Returns:
                    Nothing
    '''

    # converting the dataframe object to DateTime type Format
    final_df['date_of_purchase'] = [datetime.strptime(dates.split(" ")[0], '%Y-%m-%d') for dates in final_df['date_of_purchase']]

    # sorting the dataframe date-wise
    final_df = final_df.sort_values(by='date_of_purchase')

    
    # creating list(weeks) of sunday's dates for week-wise segration along with the start and end date

    # initializing start date if it's not sunday
    if final_df['date_of_purchase'][0].isoweekday() != 7:
        weeks = [final_df['date_of_purchase'][0]]
    else:
        weeks = []
    
    # variable to remove duplicate dates (by storing previous date and check with it)
    prev_date = str(final_df['date_of_purchase'][0])
    
    # checking for sundays
    for element in final_df['date_of_purchase']:
        if element.isoweekday() == 7 and prev_date != str(element):
            weeks.append(element)
            prev_date = str(element)

    # appending end date if it's not sunday
    if final_df['date_of_purchase'][len(final_df)-1].isoweekday() != 7:
        weeks.append(final_df['date_of_purchase'][len(final_df)-1])

    # iterating over the weeks variable for weekly separation and passing it to generate_json() function 
    for date_index in range(len(weeks)-1):
        
        if date_index == 0:
            generate_json(final_df[(final_df['date_of_purchase'] >= weeks[date_index]) & (final_df['date_of_purchase'] <= weeks[date_index + 1])], str(weeks[date_index + 1])) 
        else:
            generate_json(final_df[(final_df['date_of_purchase'] > weeks[date_index]) & (final_df['date_of_purchase'] <= weeks[date_index + 1])], str(weeks[date_index + 1]))



def merge_dataframes(product_df, customer_df, transaction_df) -> pd.DataFrame:
    
    '''
    Merging relevant columns of product and customer dataframe with transaction dataframe.

            Parameters:
                    product_df (Pandas: DataFrame): Products dataframe
                    customer_df (Pandas: DataFrame): Customers dataframe
                    transaction_df (Pandas: DataFrame): Transactions dataframe

            Returns:
                    final_df (Pandas: DataFrame): final merged dataframe
    '''

    # merging transactions dataframe with "product_category" column of product dataframe on same product_id
    intermediate_df = transaction_df.merge(product_df[["product_id", "product_category"]], on="product_id", how="left")

    # merging resultant dataframe with "loyalty_score" column of customer dataframe on same customer_id
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

    # week-wise segregation
    segregate_weekly(final_df)


if __name__ == "__main__":
    main()
