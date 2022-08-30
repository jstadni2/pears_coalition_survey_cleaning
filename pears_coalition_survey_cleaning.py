import os
import pandas as pd
import numpy as np

import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

# Calculate the path to the root directory of this script
ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '.'))

# Define path to directory for reformatted PEARS module exports
# Used output path from pears_nightly_export_reformatting.py
# Otherwise, custom field labels will cause errors
# pears_export_path = r"\path\to\reformatted_pears_data"
# Script demo uses /example_inputs directory
pears_export_path = ROOT_DIR + "/example_inputs"

Coalitions_Export = pd.ExcelFile(pears_export_path + '/' + "Coalition_Export.xlsx")
Coa_Data = pd.read_excel(Coalitions_Export, 'Coalition Data')
Coa_Data = Coa_Data.loc[Coa_Data['program_area'].isin(['SNAP-Ed', 'Family Consumer Science'])]
Coa_Data['coalition_id'] = Coa_Data['coalition_id'].astype(str)
Coa_Meetings = pd.read_excel(Coalitions_Export, 'Meetings')
Coa_Meetings['coalition_id'] = Coa_Meetings['coalition_id'].astype(str)
Coa_Meetings['start_date'] = pd.to_datetime(Coa_Meetings['start_date'])
Coa_Meetings = Coa_Meetings.sort_values(by='start_date').drop_duplicates(subset='coalition_id', keep='first')
Coa_Meetings_Data = pd.merge(Coa_Data, Coa_Meetings[['coalition_id', 'start_date']], how='left', on='coalition_id')

prev_month = (pd.to_datetime("today") - pd.DateOffset(months=1)).strftime('%m')
fq_lookup = pd.DataFrame({'fq': ['Q1', 'Q2', 'Q3', 'Q4'], 'month': ['12', '03', '06', '09'],
                          'survey_fq': ['Quarter 1 (October-December)', 'Quarter 2 (January-March)',
                                        'Quarter 3 (April-June)', 'Quarter 4 (July-September)']})
fq = fq_lookup.loc[fq_lookup['month'] == prev_month, 'fq'].item()
survey_fq = fq_lookup.loc[fq_lookup['month'] == prev_month, 'survey_fq'].item()

Coa_Surveys_Path = pears_export_path + "/Responses By Survey - Coalition Survey - " + fq + ".xlsx"
Coa_Surveys = pd.read_excel(Coa_Surveys_Path,
                            sheet_name='Response Data')
# filter Responses By Survey by Completed == ---- to export all responses
Coa_Surveys = Coa_Surveys.loc[(Coa_Surveys['For which Quarter are you completing this survey?&nbsp;'] == survey_fq) &
                              (~Coa_Surveys['coalition_name'].str.contains('(?i)TEST', regex=True, na=False)),
                              ['Program Activity ID', 'Program Name', 'Unique PEARS ID of Response', 'staff_email',
                               'What is the Coalition ID from the PEARS Coalition module that corresponds to this survey?',
                               'coalition_name', 'For which Quarter are you completing this survey?&nbsp;']]
Coa_Surveys = Coa_Surveys.rename(columns={'Program Activity ID': 'program_id',
                                          'Program Name': 'program_name',
                                          'staff_email': 'reported_by_email',
                                          'What is the Coalition ID from the PEARS Coalition module that corresponds to this survey?': 'coalition_id',
                                          'Unique PEARS ID of Response': 'response_id',
                                          'For which Quarter are you completing this survey?&nbsp;': 'survey_quarter'})
# Remove all characters besides digits from coalition_id
Coa_Surveys['coalition_id'] = Coa_Surveys['coalition_id'].astype(str)
Coa_Surveys.loc[~Coa_Surveys['coalition_id'].str.isnumeric(), 'coalition_id'] = Coa_Surveys['coalition_id'].str.extract(
    '(\d+)', expand=False)
# Auto export?
# Responses by Survey filters: Name == Coalition Survey & Survey Status == Active
# Export filters: Reporting Period == Extension 2021 & Type of Export == Individual Responses

# Import Update Notifications, used for the Corrections Report
Update_Notes = pd.read_excel(ROOT_DIR + "/example_inputs/Update Notifications.xlsx",
                             sheet_name='Quarterly Data Cleaning').drop(columns='Tab')

# Import and consolidate staff lists
# Data cleaning is only conducted on records related to SNAP-Ed and Family Consumer Science programming

FY22_INEP_Staff = pd.ExcelFile(ROOT_DIR + "/example_inputs/FY22_INEP_Staff_List.xlsx")
SNAP_Ed_Staff = pd.read_excel(FY22_INEP_Staff, sheet_name='SNAP-Ed Staff List', header=0)
HEAT_Staff = pd.read_excel(FY22_INEP_Staff, sheet_name='HEAT Project Staff', header=0)
State_Staff = pd.read_excel(FY22_INEP_Staff, sheet_name='FCS State Office', header=0)
staff_cols = ['NAME', 'E-MAIL']
staff_dfs = [SNAP_Ed_Staff[staff_cols], HEAT_Staff[staff_cols], State_Staff[staff_cols]]
INEP_Staff = pd.concat(staff_dfs, ignore_index=True).rename(columns={'E-MAIL': 'email'})
INEP_Staff = INEP_Staff.loc[~INEP_Staff.isnull().any(1)]
INEP_Staff['NAME'] = INEP_Staff['NAME'].str.split(pat=', ')
INEP_Staff['first_name'] = INEP_Staff['NAME'].str[1]
INEP_Staff['last_name'] = INEP_Staff['NAME'].str[0]
INEP_Staff['full_name'] = INEP_Staff['first_name'].map(str) + ' ' + INEP_Staff['last_name'].map(str)
CPHP_Staff = pd.read_excel(FY22_INEP_Staff, sheet_name='CPHP Staff List', header=0).rename(
    columns={'Last Name': 'last_name',
             'First Name': 'first_name',
             'Email Address': 'email'})
CPHP_Staff['full_name'] = CPHP_Staff['first_name'].map(str) + ' ' + CPHP_Staff['last_name'].map(str)
staff = INEP_Staff.drop(columns='NAME').append(
    CPHP_Staff.loc[~CPHP_Staff['email'].isnull(), ['email', 'first_name', 'last_name', 'full_name']],
    ignore_index=True).drop_duplicates()


# function for reordering comma-separated name
# df: dataframe of staff list
# name_field: column label of name field
# reordered_name_field: column label of reordered name field
# drop_substr_fields: bool for dropping name substring fields
def reorder_name(df, name_field, reordered_name_field, drop_substr_fields=False):
    out_df = df.copy(deep=True)
    out_df[name_field] = out_df[name_field].str.split(pat=', ')
    out_df['first_name'] = out_df[name_field].str[1]
    out_df['last_name'] = out_df[name_field].str[0]
    out_df[reordered_name_field] = out_df['first_name'].map(str) + ' ' + out_df['last_name'].map(str)
    if drop_substr_fields:
        out_df = out_df.drop(columns=['first_name', 'last_name'])

    return out_df


# Create lookup table for unit to regional educators
re_lookup = pd.read_excel(FY22_INEP_Staff, sheet_name="RE's and CD's")[['UNIT #', 'REGIONAL EDUCATOR', 'NETID/E-MAIL']]
re_lookup['REGIONAL EDUCATOR'] = re_lookup['REGIONAL EDUCATOR'].str.replace(', Interim', '')
re_lookup = re_lookup.drop_duplicates()
re_lookup = reorder_name(re_lookup, 'REGIONAL EDUCATOR', 'REGIONAL EDUCATOR', drop_substr_fields=True)
re_lookup['UNIT #'] = re_lookup['UNIT #'].astype(str)

# Import lookup table for counties to unit
unit_counties = pd.read_excel(ROOT_DIR + "/example_inputs/Illinois Extension Unit Counties.xlsx")
unit_counties['Unit #'] = unit_counties['Unit #'].astype(str)

# Coalition Surveys Data Cleaning

# Coalitions

Coa_Data['coalition_unit'] = Coa_Data['coalition_unit'].str.replace('|'.join([' \(County\)', ' \(District\)', 'Unit ']),
                                                                    '', regex=True)

Coa_Data = pd.merge(Coa_Data, unit_counties, how='left', left_on='coalition_unit', right_on='County')
Coa_Data.loc[(~Coa_Data['coalition_unit'].isin(unit_counties['Unit #'])) & (
    Coa_Data['coalition_unit'].isin(unit_counties['County'])), 'coalition_unit'] = Coa_Data['Unit #']

Coa_Data = Coa_Data.loc[~Coa_Data['coalition_name'].str.contains('(?i)TEST', regex=True),
                        ['coalition_id', 'coalition_name', 'reported_by_email', 'coalition_unit', 'program_area',
                         'relationship_depth', 'created', 'modified', 'on_hiatus']].rename(
    columns={'coalition_unit': 'unit'})

Coa_Data['UPDATES'] = np.nan
Coa_Data.loc[(Coa_Data['relationship_depth'].isin(['Coalition', 'Collaboration', 'Coordination']))
             & (~Coa_Data['coalition_id'].isin(Coa_Surveys['coalition_id']))
             & (Coa_Data['on_hiatus'] != 'Yes'),
             'UPDATES'] = 'Please submit a Coalition Survey for this Coalition.'

Coa_Corrections = Coa_Data.loc[(Coa_Data['UPDATES'].notnull())].drop(
    columns=['program_area', 'created', 'modified']).rename(columns={'coalition_unit': 'unit'}).fillna('')
# Send to corrections report and email

# Coalition Surveys

# How do staff update their survey responses?
# Make all staff collaborators on statewide PA?

# Data Validation:
# 'What is the coalition_id from the PEARS Coalition module that corresponds to this survey?' == numeric only

Coa_Surveys['EVALUATION TAB UPDATES'] = np.nan
Coa_Surveys.loc[~Coa_Surveys['coalition_id'].isin(Coa_Data['coalition_id']),
                'EVALUATION TAB UPDATES'] = 'Coalition ID must be an exact match of the PEARS Coalition module that corresponds to this survey.'

Coa_Survey_Corrections1 = Coa_Surveys.loc[Coa_Surveys['EVALUATION TAB UPDATES'].notnull()].set_index(
    'program_id').fillna('')
# Send to corrections report
Coa_Survey_Corrections2 = Coa_Survey_Corrections1.drop(columns='response_id')
# Send to corrections email


# Corrections Report


Coa_Sum = Coa_Corrections.count().to_frame(name="# of Entries").reset_index().rename(columns={'index': 'Update'})
Coa_Sum = Coa_Sum.loc[Coa_Sum['Update'].str.contains('UPDATE')]
# Coa_Total = {'Update' : 'Total', '# of Entries' : len(Coa_Corrections)}
# Coa_Sum = Coa_Sum.append(Coa_Total, ignore_index=True)
Coa_Sum['Module'] = 'Coalitions'

Coa_Survey_Sum = Coa_Survey_Corrections1.count().to_frame(name="# of Entries").reset_index().rename(
    columns={'index': 'Update'})
Coa_Survey_Sum = Coa_Survey_Sum.loc[Coa_Survey_Sum['Update'].str.contains('UPDATE')]
# Coa_Survey_Total = {'Update' : 'Total', '# of Entries' : len(Coa_Survey_Corrections1)}
# Coa_Survey_Sum = Coa_Survey_Sum.append(Coa_Survey_Total, ignore_index=True)
Coa_Survey_Sum['Module'] = 'Program Activities'

Corrections_Sum = Coa_Sum.append(Coa_Survey_Sum, ignore_index=True)
Corrections_Sum.insert(0, 'Module', Corrections_Sum.pop('Module'))

Corrections_Sum = pd.merge(Corrections_Sum, Update_Notes, how='left', on=['Module', 'Update'])

out_path = ROOT_DIR + "/example_outputs"

report_filename = 'Quarterly Coalition Survey Entry ' + fq + '.xlsx'
report_file_path = out_path + '/' + report_filename

report_dfs = {
    'Corrections Summary': Corrections_Sum,
    'Coalitions': Coa_Corrections,
    'Coalition Surveys': Coa_Survey_Corrections1
}


def write_report(file_path, dfs_dict):
    writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
    for sheetname, df in dfs_dict.items():  # loop through `dict` of dataframes
        df.to_excel(writer, sheet_name=sheetname, index=False, freeze_panes=(1, 0))  # send df to writer
        worksheet = writer.sheets[sheetname]  # pull worksheet object
        worksheet.autofilter(0, 0, 0, len(df.columns) - 1)
        for idx, col in enumerate(df):  # loop through all columns
            series = df[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
            )) + 1  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width
    writer.save()


write_report(report_file_path, report_dfs)

# Email Survey Notifications

# Set the following variables with the appropriate credentials and recipients
admin_username = 'your_username@domain.com'
admin_password = 'your_password'
admin_send_from = 'your_username@domain.com'
report_cc = 'list@domain.com, of_recipients@domain.com'

deadline_date = pd.to_datetime("today").replace(day=19).strftime('%A %b %d, %Y')

notification_html = """<html>
  <head></head>
<body>
            <p>
            Hello {0},<br><br>

            You are receiving this email because you need to submit or update quarterly Coalition Surveys.
            Please update the entries listed in the table(s) below by <b>5:00pm {1}</b>.      
            <ul>
              <li>Coalition Surveys are required for any Coalition in the Coordination, Coalition, or Collaboration stage of development.</li>
              <li>Use the following link to submit <b>new</b> Coalition Surveys for each Coalition listed below. <a href="https://bit.ly/3qXvAAO">https://bit.ly/3qXvAAO</a></li> 
              <li>For each entry listed, please make the edit(s) displayed in the columns labeled <b>UPDATE</b> in the column heading.</li> 
              <li>You can locate entries in PEARS by entering their IDs into the search filter.</li>              
              <li>As a friendly reminder – following the Cheat Sheets <a href="https://uofi.app.box.com/folder/49632670918?s=wwymjgjd48tyl0ow20vluj196ztbizlw">[Located Here]</a>
              will help to prevent future PEARS corrections.</li>
          </ul>

          {2}   

            <br>{3}<br>
            {4}<br>
            </p>
  </body>
</html>
"""


# Send an email with or without a xlsx attachment
# send_from: string for the sender's email address
# send_to: string for the recipient's email address
# Cc: string of comma-separated cc addresses
# subject: string for the email subject line
# html: string for the email body
# username: string for the username to authenticate with
# password: string for the password to authenticate with
# isTls: boolean, True to put the SMTP connection in Transport Layer Security mode (default: True)
# wb: boolean, whether an Excel file should be attached to this email (default: False)
# file_path: string for the xlsx attachment's filepath (default: '')
# filename: string for the xlsx attachments filename (default: '')
def send_mail(send_from,
              send_to,
              cc,
              subject,
              html,
              username,
              password,
              is_tls=True,
              wb=False,
              file_path='',
              filename=''):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = send_to
    msg['Cc'] = cc
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.attach(MIMEText(html, 'html'))

    if wb:
        fp = open(file_path, 'rb')
        part = MIMEBase('application', 'vnd.ms-excel')
        part.set_payload(fp.read())
        fp.close()
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(part)

    smtp = smtplib.SMTP('smtp.office365.com', 587)
    if is_tls:
        smtp.starttls()
    try:
        smtp.login(username, password)
        smtp.sendmail(send_from, send_to.split(',') + msg['Cc'].split(','), msg.as_string())
    except smtplib.SMTPAuthenticationError:
        print("Authentication failed. Make sure to provide a valid username and password.")
    smtp.quit()


# Create dataframe of staff to notify
notify_staff = Coa_Corrections[['reported_by_email', 'unit']].append(Coa_Survey_Corrections2[['reported_by_email']],
                                                                     ignore_index=True).drop_duplicates(
    subset='reported_by_email', keep='first')
# notify_staff = Coa_Survey_Corrections2[['reported_by_email']].drop_duplicates()

# Subset current staff using the staff list
current_staff = notify_staff.loc[notify_staff['reported_by_email'].isin(staff['email']), ['reported_by_email', 'unit']]
current_staff = current_staff.values.tolist()


# Function to subset module corrections for a specific staff member
# df: dataframe of module corrections
# former: boolean, True if subsetting corrections for a former staff member
# staff_email: string for the staff member's email
def staff_corrections(df, former=True, staff_email=''):
    if former:
        return df.loc[df['reported_by_email'].isin(former_staff['reported_by_email'])].reset_index()
    else:
        return df.loc[df['reported_by_email'] == staff_email].drop(columns=['reported_by', 'reported_by_email', 'unit'])


# Function to insert a staff member's corrections into a html email template
# dfs: dicts of module names to staff members' corrections dataframes for that module
# strs: list of strings that will be appened to the html email template string
def insert_dfs(dfs, strs):
    for heading, df in dfs.items():
        if not df.empty:
            strs.append('<h1> ' + heading + ' </h1>' + df.to_html(border=2, justify='center'))
        else:
            strs.append('')


# If email fails to send, the recipient is added to this list
failed_recipients = []

# Email Update Notifications to current staff

for x in current_staff:
    recipient = x[0]
    unit = x[1]

    Coa_df = staff_corrections(Coa_Corrections, former=False, staff_email=recipient)
    PA_df = staff_corrections(Coa_Survey_Corrections2, former=False, staff_email=recipient)

    staff_name = staff.loc[staff['email'] == recipient, 'full_name'].item()

    notification_subject = 'Coalition Survey Entry ' + fq + ', ' + staff_name

    response_tag = """If you have any questions or need help please reply to this email and a member of the FCS Evaluation Team will reach out soon.
            <br>Thanks and have a great day!<br>     

            <br> <b> FCS Evaluation Team </b> <br>
            <a href = "mailto: your_username@domain.com ">your_username@domain.com </a><br>
    """

    new_Cc = Cc

    if (unit in re_lookup["UNIT #"].tolist()) and (send_to not in State_Staff['E-MAIL'].tolist()) and (
            '@uic.edu' not in send_to):
        response_tag = 'If you have any questions or need help please contact your Regional Specialist, <b>{0}</b> (<a href = "mailto: {1} ">{1}</a>).'
        re_name = re_lookup.loc[re_lookup['UNIT #'] == unit, 'REGIONAL EDUCATOR'].item()
        re_email = re_lookup.loc[re_lookup['UNIT #'] == unit, 'NETID/E-MAIL'].item()
        response_tag = response_tag.format(*[re_name, re_email])
        new_Cc = Cc + ', ' + re_email

    notification_dfs = {'Coalitions': Coa_df, 'Coalition Surveys': PA_df}

    y = [staff.loc[staff['email'] == recipient, 'first_name'].item(), deadline_date, response_tag]

    insert_dfs(dfs, y)
    new_notification_html = notification_html.format(*y)

    # Try to send the email, otherwise add the recipient to failed_recipients
    try:
        send_mail(send_from=admin_send_from,
                  send_to=recipient,
                  cc=new_Cc,
                  subject=notification_subject,
                  html=new_notification_html,
                  username=admin_username,
                  password=admin_password,
                  wb=False,
                  is_tls=True)
    except smtplib.SMTPException:
        failed_recipients.append([staff_name, x])

# Email Update Notifications for former staff

# Subset former staff using the staff list
former_staff = notify_staff.loc[~notify_staff['reported_by_email'].isin(staff['email'])]

Coa_df = staff_corrections(Coa_Corrections, former=True)
PA_df = staff_corrections(Coa_Survey_Corrections2, former=True)

former_staff_dfs = {'Coalitions': Coa_df, 'Coalition Surveys': PA_df}

former_staff_subject = 'Former Staff Coalition Survey Entry ' + fq

former_staff_filename = former_staff_subject + '.xlsx'
former_staff_file_path = out_path + '/' + former_staff_filename

# Export former staff corrections as an Excel file
write_report(former_staff_file_path, former_staff_dfs)

# Send former staff updates email

former_staff_report_recipients = 'recipient@domain.com'

former_staff_html = """<html>
  <head></head>
<body>
            <p>
            Hello DATA ENTRY SUPPORT et al,<br><br>

            The attached Excel workbook compiles Coalition entries created by former staff that require Coalition Surveys and surveys that require updates.
            Please complete the updates for each record by <b>5:00pm {0}</b>.          
            <ul>
              <li>Use the following link to submit <b>new</b> Coalition Surveys for each Coalition listed below. <a href="https://bit.ly/3qXvAAO">https://bit.ly/3qXvAAO</a></li> 
              <li>For each entry listed, please make the edit(s) written in the columns labeled <b>UPDATE</b> in the column heading.</li> 
              <li>You can locate entries in PEARS by entering their IDs into the search filter.</li>              
            </ul>
          If you have any questions or need help please reply to this email and a member of the FCS Evaluation Team will reach out soon.

            <br>Thanks and have a great day!<br>       
            <br> <b> FCS Evaluation Team </b> <br>
            <a href = "mailto: your_username@domain.com ">your_username@domain.com </a><br>

            </p>
  </body>
</html>
"""
y = [deadline_date]

new_former_staff_html = former_staff_html.format(*y)

try:
    if not Coa_df.empty:
        send_mail(send_from=admin_send_from,
                  send_to=former_staff_report_recipients,
                  cc=Cc,
                  subject=former_staff_subject,
                  html=new_former_staff_html,
                  username=admin_username,
                  password=admin_password,
                  wb=True,
                  file_path=former_staff_file_path,
                  filename=former_staff_filename,
                  is_tls=True)
except smtplib.SMTPException:
    failed_recipients.append(['DATA ENTRY SUPPORT NAME', former_staff_report_recipients])

report_recipients = 'list@domain.com, of_recipients@domain.com'

report_subject = 'Quarterly Coalition Survey Entry Q2 ' + fq

report_html = """<html>
  <head></head>
<body>
            <p>
            Hello everyone,<br><br>

            The attached reported compiles the most recent round of quarterly Coalition Survey entry.
            If you have any questions, please reply to this email and a member of the FCS Evaluation Team will reach out soon.<br>

            <br>Thanks and have a great day!<br>       
            <br> <b> FCS Evaluation Team </b> <br>
            <a href = "mailto: your_username@domain.com ">your_username@domain.com </a><br>
            </p>
  </body>
</html>
"""

try:
    send_mail(send_from=admin_send_from,
              send_to=report_recipients,
              cc=Cc,
              subject=report_subject,
              html=report_html,
              username=admin_username,
              password=admin_password,
              wb=True,
              file_path=report_file_path,
              filename=report_filename,
              is_tls=True)
except smtplib.SMTPException:
    print("Failed to send report to Regional Specialists.")

if failed_recipients:
    fail_html = """The following recipients failed to receive an email:<br>
    {}    
    """
    new_string = '<br>'.join(map(str, failed_recipients))
    new_fail_html = html.format(new_string)
    send_mail(send_from=admin_send_from,
              send_to=admin_send_from,
              cc=Cc,
              subject='Coalition Survey Entry ' + fq + ' Failure Notice',
              html=new_fail_html,
              username=admin_username,
              password=admin_password,
              wb=False,
              is_tls=True)
else:
    print("Data cleaning notifications sent successfully.")
