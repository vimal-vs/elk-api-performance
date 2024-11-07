import config

import requests
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

ROUND_OFFSET = 3

def send_email(file):
    recipients = config.recipients
    
    subject = f"ELK API Performance Analysis Report | {config.month}"
    body = (
    f"Hello Team,\n\n"
    f"We are pleased to inform you that the comprehensive performance analysis report of the APIs from ELK "
    f"for the month of {config.month} has been successfully generated.\n\n"
    f"This report provides valuable insights into the API performance metrics, allowing us to track trends, identify any issues, "
    f"and make data-driven decisions to enhance our system's efficiency.\n\n"
    f"The detailed report has been attached to this email for your review.\n\n"
    f"\n\nPlease note: This is an automated email sent to the team members. For any queries or feedback, please contact the appropriate team member directly.\n\n"
    )

    msg = MIMEMultipart()
    msg['From'] = config.smtp_sender
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    file_path = file

    with open(file_path, 'rb') as f:
        part = MIMEApplication(f.read(), _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        part.add_header('Content-Disposition', 'attachment', filename=file)
        msg.attach(part)

    msg['To'] = ', '.join(recipients)

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(config.smtp_sender, config.smtp_password)
            server.send_message(msg)
            print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

def fetch_data(url, username, password, body):
    print("\n\nFetching Data...")
    try:
        response = requests.get(url, auth=(username, password), json=body)
        response.raise_for_status()
        print("\n\nData fetched successfully!")
        return response.json()
    except:
        print("\n\nError occured. Unable to fetch data!\n\n")
        exit(0)

def transform_data(api_data):
    print("\n\nStarting data transformation...")

    buckets = api_data.get("aggregations", {}).get("pivot", {}).get("buckets", [])
    transformed_data = []

    for bucket in buckets:

        transaction_name = bucket.get("key")
        service_name = bucket.get("2a589d32-36c7-47f8-8091-7f76a62988a8", {}).get("buckets", [{}])[0].get("465d01d7-90cc-4b1a-9cb4-be858e87d5d6", {}).get("docs", {}).get("hits", {}).get("hits", {})[0].get("fields", {}).get("service.name", {})[0]
        top_source = bucket.get("7c02e560-793b-11ee-87c7-d761745a3d39", {}).get("buckets", [{}])[0].get("7c02e561-793b-11ee-87c7-d761745a3d39", {}).get("docs", {}).get("hits", {}).get("hits", {})
        if top_source:
            top_source = top_source[0].get("fields", {}).get("http.request.headers.Source", {})[0]
        else:
            top_source = "-"
        doc_count = bucket.get("doc_count", 0)

        status_code_2xx_3xx = bucket.get("16fc97d0-793e-11ee-87c7-d761745a3d39", {}).get("buckets", [{}])[0].get("16fc97d1-793e-11ee-87c7-d761745a3d39-numerator", 0).get("doc_count", 0) 
        
        error_4xx = bucket.get("6c85f0e0-793c-11ee-87c7-d761745a3d39", {}).get("doc_count", 0)

        error_percentage_4xx = error_4xx / doc_count * 100
        error_percentage_4xx = round(error_percentage_4xx, ROUND_OFFSET)

        error_5xx_or_greater = bucket.get("386c9470-793d-11ee-87c7-d761745a3d39", {}).get("doc_count", 0)

        error_percentage_5xx_or_greater = error_5xx_or_greater / doc_count * 100
        error_percentage_5xx_or_greater = round(error_percentage_5xx_or_greater, ROUND_OFFSET)
        
        percentile_90ms = bucket.get("e4e5aaf0-c0e3-11ee-a0e1-bfebc156e073", {}).get("buckets", [{}])[0].get("e4e5aaf1-c0e3-11ee-a0e1-bfebc156e073", {}).get("values", {}).get("90.0", 0)
        percentile_90ms = round(percentile_90ms / 1000, 0)

        api_success_percentage = bucket.get("16fc97d0-793e-11ee-87c7-d761745a3d39", {}).get("buckets", [{}])[0].get("16fc97d1-793e-11ee-87c7-d761745a3d39", 0).get("value", 0)
        api_success_percentage = round(api_success_percentage * 100, ROUND_OFFSET)

        total_api_error_rate = bucket.get("008503f0-da20-11ee-961e-797e4d320638", {}).get("buckets", [{}])[0].get("008503f1-da20-11ee-961e-797e4d320638-numerator", 0).get("doc_count", 0)
        total_api_error_rate = round(total_api_error_rate, ROUND_OFFSET)

        total_api_error_rate_mismatch_percentage = bucket.get("008503f0-da20-11ee-961e-797e4d320638", {}).get("buckets", [{}])[0].get("008503f1-da20-11ee-961e-797e4d320638", 0).get("value", 0)
        total_api_error_rate_mismatch_percentage = round(total_api_error_rate_mismatch_percentage * 100, ROUND_OFFSET)

        transaction = {
            "Transaction Names of APIs": transaction_name,
            "Service Name": service_name,
            "Top Source": top_source,
            "Count": doc_count,
            "Status Code: 2xx and 3xx": status_code_2xx_3xx,
            "API Success Percentage": api_success_percentage,
            "Error: 4xx": error_4xx,
            "Error Percentage: 4xx": error_percentage_4xx,
            "Error: 5xx and greater": error_5xx_or_greater,
            "Error Percentage: 5xx and greater": error_percentage_5xx_or_greater,
            "90th Percentile (ms)": percentile_90ms,
            "Total API Error Rate": total_api_error_rate,
            "Total API Error Rate Mismatch Percentage": total_api_error_rate_mismatch_percentage,
        }
        
        transformed_data.append(transaction)

    print("\n\nData transformation completed!")
    return pd.DataFrame(transformed_data)

def save_to_excel_with_formatting(dataframe, file_name):
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
    dataframe.to_excel(writer, index=False, sheet_name='Sheet1')

    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    number_columns = [
        "Count",
        "Status Code: 2xx and 3xx",
        "Error: 4xx",
        "Error: 5xx and greater"
    ]

    error_columns = [
        "Error: 4xx",
        "Error: 5xx and greater",
        "Error Percentage: 4xx",
        "Error Percentage: 5xx and greater",
        "Total API Error Rate Mismatch Percentage"
    ]

    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'center',
        'align': 'center',
        'border': 1,
        'bg_color': '#D7E4BC'
    })

    cell_format = workbook.add_format({
        'text_wrap': True,
        'valign': 'center',
        'align': 'center',
        'border': 1
    }) 
    
    number_format = workbook.add_format({
        'num_format': '#,##0',
        'valign': 'center',
        'align': 'center',
        'border': 1
    })

    format_green = workbook.add_format({
        'num_format': '#,##0',
        'color': 'green',
        'valign': 'center',
        'align': 'center',
        'border': 1
    })

    format_red = workbook.add_format({
        'color': 'red',
        'valign': 'center',
        'align': 'center',
        'border': 1
    })

    for col_num, value in enumerate(dataframe.columns.values):
        worksheet.write(0, col_num, value, header_format)

    for row_num in range(1, len(dataframe) + 1):
        for col_num, col_name in enumerate(dataframe.columns):
            cell_value = dataframe.iloc[row_num - 1, col_num]
            if col_name in number_columns:
                worksheet.write(row_num, col_num, cell_value, number_format)
            else:
                worksheet.write(row_num, col_num, cell_value, cell_format)
                
            if col_name == "Status Code: 2xx and 3xx":
                if cell_value <= 0 or cell_value == "0":
                    worksheet.write(row_num, col_num, cell_value, format_red)
                else:
                    worksheet.write(row_num, col_num, cell_value, format_green)

    for idx, col in enumerate(dataframe.columns):
        max_len = max(dataframe[col].astype(str).map(len).max(),len(str(col))) + 2
        worksheet.set_column(idx, idx, max_len)

    writer._save()
    print("\n\nData saved in | " + file_name + " |\n")
    send_email(file_name)

def main():
    print("\n" + "-"*50)

    print("\nStart Date: " + config.start_date + "\nEnd Date: " + config.end_date)
    api_data = fetch_data(config.elk_url, config.elk_username, config.elk_password, config.body)
    dataframe = transform_data(api_data)
    save_to_excel_with_formatting(dataframe, f'ELK_API_Performance_{config.month}.xlsx')
    
    print("-"*50 + "\n")

if __name__ == "__main__":
    main()