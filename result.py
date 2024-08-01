import json
import pdfplumber
import re
import tabula
import pandas as pd

def extract_data(text):
    data = {}

    # Define regular expressions for the data
    patterns = {
        'Programme Name': r'Programme Name:\s*([^\n]*?)(?=Sem\./Year|$)',
        'Sem./Year': r'Sem\./Year:\s*([^\n]*?)(?=Batch|$)',
        'Batch': r'Batch:\s*([^\n]*?)(?=Examination|$)',
        'Examination': r'Examination:\s*([^\n]*)',
        'Institution': r'Institution:\s*([^\n]*?)(?=CS/Remarks|$)'
    }

    # Extract data using regular expressions
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            data[key] = match.group(1).strip()
        else:
            data[key] = None

    return data

def split_marks(marks):
    # Split marks into a list of strings
    marks_list = marks.split()

    # Separate into even and odd positions
    even_position_marks = [marks_list[i] for i in range(len(marks_list)) if i % 2 == 0]
    odd_position_marks = [marks_list[i] for i in range(len(marks_list)) if i % 2 != 0]

    return even_position_marks, odd_position_marks


def find_columns_between(df, start_column='Roll no./Name', end_column='CS/Remarks'):
    try:
        # Get the index of the start and end columns
        start_index = df.columns.get_loc(start_column)
        end_index = df.columns.get_loc(end_column)

        # Ensure start_index is less than end_index
        if start_index > end_index:
            raise ValueError("Start column is after the end column.")

        # Extract column names between the start and end columns
        columns_between = df.columns[start_index + 1:end_index]
        return columns_between.tolist()

    except KeyError as e:
        print(f"Column not found: {e}")
        return []

def split_paperid(paperid_list):
    total_str = ','.join(paperid_list).strip('[]')
    paperid_part = [part.split('(')[0].strip() for part in total_str.split(',')]
    credits_part = [(part.split('(')[1].strip()).split(')')[0].strip() for part in total_str.split(',')]
    return paperid_part, credits_part

def clean_total(total_list):
    # Join the list into a single string, remove square brackets, and split by commas
    total_str = ','.join(total_list).strip('[]')

    # Extract the numbers by splitting the string and removing any non-digit characters
    numbers = [part.split('(')[0].strip() for part in total_str.split(',')]

    return numbers

def get_grade_point(marks):
    if marks >= 90:
        return 10
    elif marks >= 75:
        return 9
    elif marks >= 65:
        return 8
    elif marks >= 55:
        return 7
    elif marks >= 50:
        return 6
    elif marks >= 45:
        return 5
    elif marks >= 40:
        return 4
    else:
        return 0

def calculate_cgpa(row):
  if row['Examination'] != 'REGULAR':
    return None
  else:
    total_credits = 0
    product_cred_marks = 0
    for i in range(len(row['Total'])):
        try:
          marks = int(row['Total'][i])
        except:
          continue
        credits = int(row['Credits'][i])
        grade = get_grade_point(marks)
        product_cred_marks += credits*grade
        total_credits += credits
    try:
      cgpa = product_cred_marks/total_credits
      cgpa = round(cgpa, 2)
    except:
      cgpa = 0

    return cgpa
  
def cleaning_preprocessing(df):
    columns_between = find_columns_between(df)

    structured_data = []

    step_size = 5

    for i in range(0, len(df), step_size):
        if i + 4 < len(df):
            enrollment_no = df.iloc[i]['Roll no./Name']
            name = df.iloc[i + 1]['Roll no./Name']
            s_no = df.iloc[i + 4]['S.No.']
            Programme_Name = df.iloc[i + 4]['Programme Name']
            Sem = df.iloc[i + 4]['Sem./Year']
            Batch = df.iloc[i + 4]['Batch']
            Examination = df.iloc[i + 4]['Examination'].split(' ')[0].strip()

            if pd.isna(name):
              continue


            paper_id_parts = []

            row_data = df.iloc[i][columns_between].fillna('').astype(str).tolist()
            paper_id_parts.extend(row_data)
            paper_id = ' '.join(paper_id_parts)

            int_ext_marks_parts = []
            row_data = df.iloc[i+2][columns_between].fillna('').astype(str).tolist()
            int_ext_marks_parts.extend(row_data)
            int_ext_marks = ' '.join(int_ext_marks_parts)

            total_marks_parts = []
            row_data = df.iloc[i+4][columns_between].fillna('').astype(str).tolist()
            total_marks_parts.extend(row_data)
            total_marks = ' '.join(total_marks_parts)

            # Append the extracted data to the structured_data list
            structured_data.append([s_no,Batch,Programme_Name,Sem,Examination, name, enrollment_no, paper_id, int_ext_marks, total_marks])

    # Create a new DataFrame from the structured data
    structured_df = pd.DataFrame(structured_data, columns=['S.No.','Batch','Programme_Name','Sem','Examination', 'Name', 'Enrollment No.', 'PaperID', 'Marks', 'Total'])
    structured_df['PaperID'] = structured_df['PaperID'].apply(lambda x: x.split())
    structured_df['Total'] = structured_df['Total'].apply(lambda x: x.split())
    structured_df[['Int_Marks', 'Ext_Marks']] = structured_df['Marks'].apply(lambda x: pd.Series(split_marks(x)))
    structured_df[['PaperID', 'Credits']] = structured_df['PaperID'].apply(split_paperid).apply(pd.Series)
    structured_df['Total'] = structured_df['Total'].apply(clean_total)
    # Drop the original 'Marks' column if no longer needed
    structured_df = structured_df.drop(columns=['Marks'])
    # Add the 'CGPA' column
    structured_df['CGPA'] = structured_df.apply(calculate_cgpa, axis=1)

    return structured_df


def format1(file_stream):
    all_data = []
    institution_pages = []

    # Use BytesIO for in-memory file-like object
    with pdfplumber.open(file_stream) as pdf:
        # Iterate through all pages
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            # Extract data from the text of the current page
            extracted_data = extract_data(text)

            print(i + 1)
            # Check if Institution matches the required value
            if extracted_data.get('Institution') == 'BHAGWAN PARSHURAM INSTITUTE OF TECHNOLOGY':
                institution_pages.append(i + 1)  # Store the page number (1-indexed)
                all_data.append(extracted_data)
            else:
                continue

    previous_metadata = None
    previous_df = None
    result_dfs = []
    j = 0

    # Extract tables if Institution matches
    if institution_pages:
        # Use BytesIO for in-memory file-like object with tabula
        file_stream.seek(0)  # Reset stream position to the beginning
        tables = tabula.read_pdf(file_stream, pages=institution_pages, multiple_tables=True)

        for table in tables:
            if 'Roll no./Name' in table.columns:
                # Add metadata columns to the DataFrame
                for key in ['Programme Name', 'Sem./Year', 'Batch', 'Examination', 'Institution']:
                    table[key] = all_data[j].get(key)

                current_metadata = (
                    all_data[j].get('Programme Name'),
                    all_data[j].get('Sem./Year'),
                    all_data[j].get('Batch'),
                    all_data[j].get('Examination')
                )
                j += 1
                if previous_metadata:
                    # Compare current metadata with previous metadata
                    if current_metadata == previous_metadata:
                        # Merge with the previous DataFrame
                        previous_df = pd.concat([previous_df, table], ignore_index=True)
                    else:
                        # Save previous DataFrame to the list if not empty
                        if previous_df is not None and not previous_df.empty:
                            result_dfs.append(previous_df)
                        # Update previous metadata and DataFrame
                        previous_metadata = current_metadata
                        previous_df = table
                else:
                    previous_metadata = current_metadata
                    previous_df = table

            else:
                continue

        if previous_df is not None and not previous_df.empty:
            result_dfs.append(previous_df)
    else:
        print("No pages with the specified Institution found.")

    cleaned_result_dfs = []
    for df in result_dfs:
        cleaned_result_dfs.append(cleaning_preprocessing(df))
    
    result = {}

    # Process each DataFrame
    for df in cleaned_result_dfs:
        for _, row in df.iterrows():
            batch = row['Batch']
            programme_name = row['Programme_Name']
            sem = row['Sem']
            examination = row['Examination']
            enrollment_no = row['Enrollment No.']
            name = row['Name']
            paper_id = row['PaperID']
            credits = row['Credits']
            int_marks = row['Int_Marks']
            ext_marks = row['Ext_Marks']
            total = row['Total']
            cgpa = row['CGPA']

            # Create nested structure
            if batch not in result:
                result[batch] = {}
            if programme_name not in result[batch]:
                result[batch][programme_name] = {}
            if sem not in result[batch][programme_name]:
                result[batch][programme_name][sem] = {}
            if examination not in result[batch][programme_name][sem]:
                result[batch][programme_name][sem][examination] = []

            # Append paper details to the list
            result[batch][programme_name][sem][examination].append({
                'Enrollment': enrollment_no,
                'Name': name,
                'CGPA': cgpa,
                'Papers': [{
                    'ID': paper_id,
                    'Credits': str(credits),
                    'Int_Marks': str(int_marks),
                    'Ext_Marks': str(ext_marks),
                    'Total': str(total)
                }]
            })

    # Save the result as a JSON file
    json_path = "result.json"
    with open(json_path, 'w') as json_file:
        json.dump(result, json_file, indent=4)

    return result