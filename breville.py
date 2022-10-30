'''
    Execution steps:
    Step 1: Unions all the project files present in the data folder. data folder can be changed to any other in the code below.
    Step 2: Summarizes the Headcount by Position using the employee_data.csv
    Step 3: Summarizes Question 1 from the unioned table and presents it as a CSV
    Step 4: Summarizes Question 2 from the unioned table and presents as a CSV
    Step 5: Transforms the data to the requested format by the Business Analyst
    Step 6: Saves the transformed csv file in the same directory
    Step 7: Loads the transformed CSV to s3 (can be dropped to a database using the copy command)
    (Note): Please pass the aws_creds into the JSON to load to S3
'''

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
from pathlib import Path  
from io import StringIO
import boto3
import json

pd.set_option('expand_frame_repr', False)
pd.options.mode.chained_assignment = None

# Change these column names in case an additional column is requested/dropped in the future
requested_columns = ['Project','Assessed','Assessor','KeepDoing','Solutions','Effective','DeliverResults','DemonstrateKnowledge','WorkAgain','Improve']

# Define where the Project files are stored
data_dir = 'data'

# Define name of bucket where the csv will be loaded
bucket = ''

# Creates filepaths for the Project files
filepath_prefix = os. getcwd() + "\\" + data_dir + "\\"
employees_path = os. getcwd() + "\\" + data_dir + "\\" + "employee_data.csv"
employees_table = pd.read_csv(employees_path)

# Defines the columns present in the Project files
project_file_columns = ['Assessed','Assessor','question1','question2','dimension','answer']

# Lists the files present in the data directory
files = os.listdir(data_dir)

class main:
    def __init__(self):
        print("Generating Report")
        final_df = self.prepare_data()
        
        dir_path = os.getcwd()
        # final_df.to_csv(dir_path + '\\' + 'final_data.csv')
        
        self.headcount(final_df)
        q1_table = self.question_1(final_df)
        q2_table = self.question_2(final_df)
        output_df = self.specific_format(q1_table,q2_table)
        # Uncomment this function to push final data to AWS s3 (Define aws creds in the aws_creds.json first)
        # self.push_to_s3(output_df)
        
    def prepare_data(self):
        print("Data prepared!")
        # Creating an empty dataframe
        final_df = pd.DataFrame(columns = project_file_columns)
        final_df['Project'] = None
        index = 0
        # Loops through the Project files in the data directory and combines the data
        for file in files:
            if file[:7] == 'Project':
                index += 1
                df_name = 'df' + str(index)
                df_name = pd.read_csv(filepath_prefix + file)
                # Assigning project name as a column
                df_name['Project'] = file[:-4]
                final_df = pd.concat([final_df,df_name], ignore_index = True)
                # Creating a column that shows whether the Peer has worked closely or not
                final_df['worked_closely'] = np.where((final_df['question2']== 'I worked closely enough with this person and feel confident to provide feedback to him/her') & (final_df['answer']=='Yes'), 1, 0)
        return final_df
    
    # Question 1 Summary
    def question_1(self,df):
        print("Generating Summary of Question 1")
        projects = df['Project'].unique()
        # Filtering relevant rows and casting to integer
        project_df_filtered = df[(df['dimension'] == 'Current Performance')]
        project_df_filtered['answer'] = project_df_filtered['answer'].astype('int')
        # Filters relevant columns
        project_df = project_df_filtered[['Project','Assessed','Assessor','question1','answer']]
        people_assessed = len(pd.unique(project_df['Assessed']))
        print('No. of People Assessed: ' + str(people_assessed))
        # Pivoting Question 1
        pivot = project_df.pivot_table('answer', ['Project','Assessor', 'Assessed'], 'question1')
        pivot.columns = ['Solutions','Effective','DeliverResults','DemonstrateKnowledge','WorkAgain']
        # Brings column names to single row
        pivot.columns.name = None
        pivot = pivot.reset_index()
        print(pivot)
            
        dir_path = os.getcwd()
        pivot.to_csv(dir_path + '\\' + 'question_1_summary.csv')
        
        return pivot
    
    def question_2(self,df):
        print("Generating Summary of Question 2")
        projects = df['Project'].unique()
        bool_series = pd.isnull(df["dimension"])
        null_dimension = df[bool_series]
        project_df_filtered = null_dimension[null_dimension["question2"] != 'Spaceholder']
        # Filters relevant columns
        project_df = project_df_filtered[['Project','Assessed','Assessor','question2','answer']]
        people_assessed = len(pd.unique(project_df['Assessed']))
        print('No. of People Assessed: ' + str(people_assessed))
        pivot = project_df.pivot_table(index=['Project', 'Assessed','Assessor'],
                             columns=['question2'],
                             values=['answer'],
                             aggfunc=lambda x: ''.join(str(v) for v in x))
        # pivot.columns = [['WorkClosely','Improve','KeepDoing','Relatinship']]
        pivot.columns = ['WorkClosely','Improve','KeepDoing','Relatinship']
        # Drops top pivot column name
        pivot.drop(index=pivot.index[0], 
        axis=0, 
        inplace=True)
        # Brings column names to single row
        pivot.columns.name = None
        pivot = pivot.reset_index()
        print(pivot)
            
        dir_path = os.getcwd()
        pivot.to_csv(dir_path + '\\' + 'question_2_summary.csv')
        
        return pivot
    
    def headcount(self,df):
        print('Position headcount details:')
        employees_table.rename(columns={'name':'Assessed'}, inplace=True)
        joined = df.merge(employees_table,on='Assessed',how='left')
        position_df_filtered = joined[(joined['dimension'] == 'Current Performance')]
        position_df = position_df_filtered[['position','Assessed']]
        grouped_df = position_df.groupby(['position']).nunique()
        grouped_df.columns = ['Headcount']
        grouped_df.columns.name = None
        grouped_df = grouped_df.reset_index()
        print(grouped_df)
    
    def specific_format(self,table1,table2):
        print('Generating Required Table')
        merged_df = table1.merge(table2, on=['Project','Assessed','Assessor'], how='left')
        #print(merged_df)
        worked_closely_filtered = merged_df[merged_df["WorkClosely"] == 'Yes']
        #print(worked_closely_filtered)
        relevant_columns_only = worked_closely_filtered[requested_columns]
        relevant_columns_only = relevant_columns_only.replace('nan', 'NA')
        print(relevant_columns_only)
        # Export as CSV
        dir_path = os.getcwd()
        relevant_columns_only.to_csv(dir_path + '\\' + 'transformed.csv',index=False)

        return relevant_columns_only
    
    def push_to_s3(self,dataframe):
        f = open('aws_creds.json')
        aws_creds = json.load(f)
        session = boto3.Session(
            aws_access_key_id=aws_creds['access_key_id'],
            aws_secret_access_key=aws_creds['secret_access_key']
        )
        
        s3 = session.resource('s3')
        bucket_name = bucket # Add s3 bucket name here
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer,index = False)
        s3.Object(bucket_name,'final_output.csv').put(Body = csv_buffer.getvalue())
        
main()