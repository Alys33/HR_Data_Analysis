import pandas as pd
import requests
import os
pd.set_option("display.max_columns", 50)


if __name__ == '__main__':

    if not os.path.exists('../Data'):
        os.mkdir('../Data')

    # Download data if it is unavailable.
    if ('A_office_data.xml' not in os.listdir('../Data') and
        'B_office_data.xml' not in os.listdir('../Data') and
        'hr_data.xml' not in os.listdir('../Data')):
        print('A_office_data loading.')
        url = "https://www.dropbox.com/s/jpeknyzx57c4jb2/A_office_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/A_office_data.xml', 'wb').write(r.content)
        print('Loaded.')

        print('B_office_data loading.')
        url = "https://www.dropbox.com/s/hea0tbhir64u9t5/B_office_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/B_office_data.xml', 'wb').write(r.content)
        print('Loaded.')

        print('hr_data loading.')
        url = "https://www.dropbox.com/s/u6jzqqg1byajy0s/hr_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/hr_data.xml', 'wb').write(r.content)
        print('Loaded.')

        # All data in now loaded to the Data folder.

    # write your code here
# Stage 1/ 5: LOAD THE DATA AND MODIFY THE INDEXES

'''loading the data'''

A_office = pd.read_xml("../Data/A_office_data.xml")
B_office = pd.read_xml("../Data/B_office_data.xml")
hr_data = pd.read_xml("../Data/hr_data.xml")

'''reindexing the data'''

A_office['n_index'] = A_office['employee_office_id'].apply(lambda x: 'A'+str(x))
A_office.index = A_office['n_index'].values
A_office.drop(columns='n_index', inplace=True)

B_office['n_index'] = B_office['employee_office_id'].apply(lambda x: 'B' + str(x))
B_office.index = B_office['n_index'].values
B_office.drop(columns='n_index', inplace=True)

hr_data.index = hr_data.employee_id

'''printing the indexes wrapped in a list'''

# print(A_office.index.tolist())
# print(B_office.index.tolist())
# print(hr_data.index.tolist())

# Stage 2/5 : MERGE EVERYTHING
'''concatenating A_office and B_office datasets'''

A_B_dataset = pd.concat([A_office, B_office])

merged_dataset = A_B_dataset.merge(hr_data, right_index=True, left_index=True, how='left', indicator=True)

'''keeping those employees whose data is contained in both datasets'''

merged_dataset = merged_dataset[merged_dataset['_merge'] == "both"]

'''dropping the columns employee_office_id, employee_id and _merge'''
merged_dataset.drop(columns=['employee_office_id', 'employee_id', '_merge'], inplace=True)
merged_dataset.sort_index(inplace=True)

'''printing the indexes and the columns names wrapped in a list'''

# print(merged_dataset.index.tolist())
# print(merged_dataset.columns.tolist())

# Stage 3/5: GET THE INSIGHTS

'''What are the departments of the top ten employees in terms of working hours?'''

hours_dept = merged_dataset[['average_monthly_hours', 'Department']]
dpt_top_ten = hours_dept.sort_values("average_monthly_hours", ascending=False)['Department'].head(10).tolist()

#print((dpt_top_ten))

'''What is the total number of projects on which IT department employees with low salaries have worked?'''
subset_on_condition = merged_dataset[(merged_dataset.Department == 'IT') & (merged_dataset.salary == 'low')]
total_num_projects = subset_on_condition['number_project'].sum()

#print(total_num_projects)

'''What are the last evaluation scores and the satisfaction levels of the employees A4, B7064, A3033?'''
empA4 = merged_dataset.loc[merged_dataset.index == 'A4', ['last_evaluation', 'satisfaction_level']].values.flatten().tolist()
empB7064 = merged_dataset.loc[merged_dataset.index == 'B7064', ['last_evaluation', 'satisfaction_level']].values.flatten().tolist()
empA3033 = merged_dataset.loc[merged_dataset.index == 'A3033', ['last_evaluation', 'satisfaction_level']].values.flatten().tolist()

#print([empA4, empB7064, empA3033])




# Stage 4/5: AGGREGATE THE DATA


'''changing the type of the column named "left"'''

def count_bigger_5(group):
    return group[group > 5].count()

def share_employee(group):
    return group[group ==1].count() / group.count()


merged_dataset.left = merged_dataset.left.astype('int')

median_count = merged_dataset.groupby('left')['number_project'].agg([count_bigger_5, 'median']).round(2).to_dict()
median_mean_ts = merged_dataset.groupby('left')['time_spend_company'].agg(['median', 'mean']).round(2).to_dict()
shared_employee = merged_dataset.groupby('left')['Work_accident'].agg([share_employee]).round(2).to_dict()
mean_std_eval_score = merged_dataset.groupby('left')['last_evaluation'].agg(['mean','std']).round(2).to_dict()

output_dict = {('number_project', 'median'): median_count['median'],
               ('number_project', 'count_bigger_5'): median_count['count_bigger_5'],
               ('time_spend_company', 'mean'): median_mean_ts['mean'],
               ('time_spend_company', 'median'): median_mean_ts['median'],
               ('Work_accident', 'mean'): shared_employee['share_employee'],
               ('last_evaluation', 'mean'): mean_std_eval_score['mean'],
               ('last_evaluation', 'std'): mean_std_eval_score['std']}

#print(output_dict)


# Stage 5/5 (Final Stage):  Draw up pivot tables

merged_dataset.left = merged_dataset.left.astype('float')

first_pivot_table = merged_dataset.pivot_table(index='Department', columns=['left', 'salary'], values='average_monthly_hours', aggfunc='median').round(2)
condition = (first_pivot_table[0]['high'] < first_pivot_table[0]['medium']) | (first_pivot_table[1]['low'] < first_pivot_table[1]['high'])
print(first_pivot_table[condition].to_dict())


second_pivot_table = merged_dataset.pivot_table(index='time_spend_company', columns='promotion_last_5years', values=['satisfaction_level', 'last_evaluation'], aggfunc=['min', 'max', 'mean']).round(2)

condition = second_pivot_table['mean']['last_evaluation'][0] > second_pivot_table['mean']['last_evaluation'][1]

print(second_pivot_table[condition].to_dict())