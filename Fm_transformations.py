
import pandas as pd
import numpy as np   
from datetime import datetime
import json

def transform_consignments(self, df):
        return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
            foc_cnno=lambda df: df['foc_cnno'].fillna('Unknown'),
            timestamp = lambda df:  df['BookingTs'].apply(convert_to_timestamp),
            foc_booking_date = lambda df: df['timestamp'].apply(lambda x: x.date() if pd.notna(x) else None),
            foc_bkdate=lambda df: pd.to_datetime(df['BookingTs'].astype(float) / 1000, unit='s', errors='coerce'),
            foc_bkoffice_id=lambda df: df['Origin_Branch_Code'],
            foc_bkdateid=lambda df:  df['BookingTs'].apply(convert_to_timestamp),
            foc_bkdate_id=lambda df: df['foc_bkdateid'],
            foc_bkserv_code=lambda df: df['Service_Code'],
            foc_bkservice_code=lambda df: df['Service_Name'], 
            foc_bkmode_code=lambda df: df['Mode'],
            foc_bkoffice_code=lambda df: df['Origin_Branch_Name'],
            timestamp_edd=lambda df:  df['Ops_EDD'].apply(convert_to_timestamp),
            foc_edd = lambda df: df['timestamp_edd'].apply(lambda x: x.date() if pd.notna(x) else None),
            foc_bkgpin_code=lambda df: df['Destination_Pincode'],
            foc_reddsla_date = lambda df: pd.to_datetime(df['Ops_REDD'].astype(float) / 1000, unit='s', errors='coerce').apply(lambda x: None if pd.isna(x) else x),
            foc_customer_promise_edd = lambda df: pd.to_datetime(pd.to_numeric(df['Cust_Prom_EDD'], errors='coerce').div(1000), unit='s', errors='coerce').dt.date,
            foc_cust_ref_no=lambda df: df['Reference_No'],
            foc_edd_booked_status = np.where(df['Ops_EDD_Parameters'].str[8] == '0', 'Booked Before Cut-Off', 'Booked After Cut-Off'),
            foc_bkpin_code=lambda df: df['Origin_Pincode'],
            foc_pickup_customer_code=lambda df: np.where(df['Current_Status_Code'] == 'PCUP', df['Customer_Code'], None),
            foc_softdata_pickup_refno=lambda df: df['Reference_No']
        )

def transform_booking_details(self, df):
        now = datetime.now()
        curent_dt = now.strftime("%Y-%m-%d %H:%M:%S") 
        return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
            foc_bkcusttype_code=lambda df: df['Booked_By_Cust_Type'],
            foc_bkcust_code=lambda df: df['Booked_By_Cust_Code'],
            foc_bookchannel=lambda df: df['Source_Application'],
            foc_bkdocument_code=lambda df: df['Package_Type'],
            foc_Booked_Wt=lambda df: df['Booking_Weight'],
            foc_Vol_Wt=lambda df: df['Volumetric_Weight'],
            foc_Billed_Wt=lambda df: df['Chargeable_Weight'],
            foc_pcs=lambda df: np.where(df['Number_of_Pieces'].notnull(), 1, 0),
            foc_nop=lambda df: df['Number_of_Pieces'],
            foc_booking_app=lambda df: df['Validation'],
            record_updated_date=lambda x: curent_dt,
            foc_cpdp_code=lambda df: np.where(df['Booked_By_Cust_Code'] == 'CPDP', 'CPDP', None),
            foc_content_desc = lambda df: np.where(df['Commodity_Details'].notna(), df['Commodity_Details'], None)
        )

def transform_payment_details(self, df):
        now = datetime.now()
        curent_dt = now.strftime("%Y-%m-%d %H:%M:%S") 
        return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
                record_updated_date=lambda x: curent_dt,
                foc_cod_flag = lambda df: np.where(df['Vas_Prod_Code'].notna(), 1, 0),
                foc_bkinvvalue = lambda df: np.where(df['Invoice_Value'].notna(), df['Invoice_Value'], None)
        )
    

def transform_address_dtails(self, df):
        now = datetime.now()
        curent_dt = now.strftime("%Y-%m-%d %H:%M:%S") 
        return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
            foc_receiver_name = np.where(df['Address_Type'] == 'Receiver', df['First_name'].fillna('') + ' ' + df['Middle_name'].fillna('') + ' ' + df['Last_name'].fillna(''), None),
            foc_receiver_company_name = np.where((df['Address_Type'] == 'Receiver') & (df['First_name'].str.contains('LTD', case=False, na=False)),df['First_name'],None),
            foc_receiver_phno = np.where(df['Address_Type'] == 'Receiver', df['Phone'].fillna('') + '/' + df['Mobile'].fillna(''), None),
            foc_receiver_address = np.where(df['Address_Type'] == 'Receiver', df['Street_1'].fillna('') + ',' + df['Street_2'].fillna('') + ',' + df['Street_3'].fillna(''), None),
            foc_receiver_city = np.where(df['Address_Type'] == 'Receiver', df['City'].fillna(''), None),
            foc_receiver_state = np.where(df['Address_Type'] == 'Receiver', df['State'].fillna(''), None),
            foc_receiver_pincode = np.where(df['Address_Type'] == 'Receiver', df['Pincode'].fillna(''), None),
            record_updated_date=lambda x: self.curent_dt,
            foc_sender_name = np.where(df['Address_Type'] == 'Sender', df['First_name'].fillna('') + ' ' + df['Middle_name'].fillna('') + ' ' + df['Last_name'].fillna(''), None),
            foc_sender_company_name = np.where((df['Address_Type'] == 'Sender') & (df['First_name'].str.contains('LTD', case=False, na=False)),df['First_name'],None),
            foc_sender_phno = np.where(df['Address_Type'] == 'Sender', df['Phone'].fillna('') + '/' + df['Mobile'].fillna(''), None),
            foc_sender_address = np.where(df['Address_Type'] == 'Sender', df['Street_1'].fillna('') + ',' + df['Street_2'].fillna('') + ',' + df['Street_3'].fillna(''), None),
            foc_sender_city = np.where(df['Address_Type'] == 'Sender', df['City'].fillna(''), None),
            foc_sender_state = np.where(df['Address_Type'] == 'Sender', df['State'].fillna(''), None),
            foc_sender_pincode = np.where(df['Address_Type'] == 'Sender', df['Pincode'].fillna(''), None)
        )
    

def Transform_Consignment_History(self, df):
    now = datetime.now()
    current_dt = now.strftime("%Y-%m-%d %H:%M:%S")  # Fixed typo here
    return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
        foc_pickup_date = lambda df: np.where(df['Status_Code'] == 'PCUP', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_booked_by = lambda df: np.where(df['Status_Code'] == 'PCUP', df['Biker_code'], None),
        foc_pickup_remarks = lambda df: np.where(df['Status_Code'] == 'PCNO', df['Reason_Code'] + ', ' + df['Reason_Description'], None),
        foc_pickup_scheduled_date = lambda df: np.where(df['Status_Code'] == 'PCSC', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_cancelled_date = lambda df: np.where(df['Status_Code'] == 'PCAN', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_branch_code = lambda df: np.where(df['Status_Code'] == 'PCUP', df['Current_Hub_Code'], None),
        foc_pickup_branch_name = lambda df: np.where(df['Status_Code'] == 'PCUP', df['Current_Hub_Name'], None),
        foc_pickup_awaited_date = lambda df: np.where(df['Status_Code'] == 'PCAW', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_picked_up_date = lambda df: np.where(df['Status_Code'] == 'PCUP', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_dropoff_awaited_date = lambda df: np.where(df['Status_Code'] == 'DRAW', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_dropoff_scheduled_date = lambda df: np.where(df['Status_Code'] == 'DRSC', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_dropoff_completed_date = lambda df: np.where(df['Status_Code'] == 'DRCOM', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_dropoff_cancelled_date = lambda df: np.where(df['Status_Code'] == 'DRCAN', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_dropoff_rejected_date = lambda df: np.where(df['Status_Code'] == 'DRREC', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_hub_type = lambda df: np.where((df['Status_Code'] == 'PCUP') & (df['Current_Location_Office_Type'] == 'HUB_TYPE'), 'HUB_TYPE', None),  # Fixed space issue in column name
        foc_pickup_city_code = lambda df: np.where(df['Status_Code'] == 'PCUP', df['Current_Location_Code'], None),
        foc_pre_bkpmfmadefor_loccode = lambda df: np.where(df['Status_Code'] == 'OPMF', df['Current_Hub_Code'], None),
        record_updated_date = lambda x: current_dt,  # Fixed variable name typo here
        foc_pickup_attempt_date = lambda df: np.where(df['Status_Code'] == 'PCNO', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_reassigned_date = lambda df: np.where(df['Status_Code'] == 'PCRA', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_attempt2_date = lambda df: np.where(df['Status_Code'] == 'PCNO', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_attempt3_date = lambda df: np.where(df['Status_Code'] == 'PCNO', df['StatusTs'].apply(convert_to_timestamp), None),
        #foc_pickup_attempt3_date = lambda df: np.where(df['Status_Code'] == 'PCNO', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_attemptl_date = lambda df: np.where(df['Status_Code'] == 'PCNO', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_scheduled2_date = lambda df: np.where(df['Status_Code'] == 'PCSC', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_scheduled3_date = lambda df: np.where(df['Status_Code'] == 'PCSC', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_scheduledl_date = lambda df: np.where(df['Status_Code'] == 'PCSC', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_reassigned2_date = lambda df: np.where(df['Status_Code'] == 'PCRA', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_reassigned3_date = lambda df: np.where(df['Status_Code'] == 'PCRA', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_reassignedl_date = lambda df: np.where(df['Status_Code'] == 'PCRA', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup2_remarks = lambda df: np.where(df['Status_Code'] == 'PCNO', df['Reason_Code'] + ', ' + df['Reason_Description'], None),
        foc_pickup3_remarks = lambda df: np.where(df['Status_Code'] == 'PCNO', df['Reason_Code'] + ', ' + df['Reason_Description'], None),
        foc_pickupl_remarks = lambda df: np.where(df['Status_Code'] == 'PCNO', df['Reason_Code'] + ', ' + df['Reason_Description'], None),
        foc_pickup_npr_reason = lambda df: np.where(df['Status_Code'] == 'PCNO', df['Reason_Code'] + ', ' + df['Reason_Description'], None),
        foc_pickup_npr2_reason = lambda df: np.where(df['Status_Code'] == 'PCNO', df['Reason_Code'] + ', ' + df['Reason_Description'], None),
        foc_pickup_npr3_reason = lambda df: np.where(df['Status_Code'] == 'PCNO', df['Reason_Code'] + ', ' + df['Reason_Description'], None),
        foc_pickup_nprl_reason = lambda df: np.where(df['Status_Code'] == 'PCNO', df['Reason_Code'] + ', ' + df['Reason_Description'], None),
        foc_pickup_cancelled2_date = lambda df: np.where(df['Status_Code'] == 'PCAN', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_cancelled3_date = lambda df: np.where(df['Status_Code'] == 'PCAN', df['StatusTs'].apply(convert_to_timestamp), None),
        foc_pickup_cancelledl_date = lambda df: np.where(df['Status_Code'] == 'PCAN', df['StatusTs'].apply(convert_to_timestamp), None),
        #foc_pre_bkbrpmfno = lambda df: np.where(df['Status_Code'] == 'OPMF', df['JSON_Data'].apply(lambda x: json.loads(x).get('Packet_Manifest_Number', None)), None),
        foc_pre_bkbrpmfno = lambda df: np.where(df['Status_Code'] == 'OPMF', df['JSON_Data'].apply(lambda x: json.loads(x).get('Packet_Manifest_Number', None) if x and isinstance(x, str) else None), None)

    )        


def convert_to_timestamp(value):
    if isinstance(value, (int, float)):  # Check if it's a numeric (epoch timestamp)
        return pd.to_datetime(value, unit='ns', errors='coerce')
    try:
        # If it's already a string or datetime-like object, convert directly
        return pd.to_datetime(value, errors='coerce')
    except (ValueError, TypeError):
        return pd.NaT
    

def convert_to_timestamp(value):
    if isinstance(value, (int, float)):
        if abs(value) > 1e13:
            return pd.to_datetime(value, unit='ns', errors='coerce')
        else:
            return pd.to_datetime(value, unit='ms', errors='coerce')
    try:
        return pd.to_datetime(value, errors='coerce')
    except (ValueError, TypeError):
        return pd.NaT  
