'''
File for testing functions in solution_start.py with the test cases
cmd to run it: pytest test.py
Some supporting files to run test cases present in './test_files'
'''

# importing libraries
import pytest
import pandas as pd
from solution import solution_start

'''
Test Cases for sorted_alphanumeric_Ids function
'''
@pytest.mark.parametrize('input_list, output_list', 
            [
                (['A12', 'A21', 'A3'], ['A3', 'A12', 'A21']),   # Test Case 1
                (['C1', 'C8', 'C4'], ['C1', 'C4', 'C8'])        # Test Case 2
            ])
def test_sorted_alphanumeric_Ids(input_list : list, output_list : list):
    assert solution_start.sorted_alphanumeric_Ids(input_list) == output_list


'''
Test Cases for separate_column_elements function
'''
@pytest.mark.parametrize('transaction_raw_df, transaction_df',
                            [(
                                pd.read_csv('./test_files/transaction_raw_df.csv'),
                                pd.read_csv('./test_files/transaction1_df.csv')
                            )]
                        )
def test_separate_column_elements(transaction_raw_df : pd.DataFrame, transaction_df : pd.DataFrame):
    tr_df = solution_start.separate_column_elements(transaction_raw_df)
    assert tr_df.equals(transaction_df)
    

'''
Test Cases for merge_dataframes function
'''
@pytest.mark.parametrize('product_df, customer_df, transaction_df, final_df', 
                        [(
                            pd.read_csv('./test_files/product1_df.csv'),
                            pd.read_csv('./test_files/customer1_df.csv'),
                            pd.read_csv('./test_files/transaction1_df.csv'),
                            pd.read_csv('./test_files/final_df.csv')
                        )])
def test_merge_dataframes(product_df : pd.DataFrame, customer_df : pd.DataFrame, transaction_df : pd.DataFrame, final_df : pd.DataFrame):
    df = solution_start.merge_dataframes(product_df, customer_df, transaction_df)
    assert df.equals(final_df)

