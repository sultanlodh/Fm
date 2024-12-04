config = {
    "consumer_settings": {
        "bootstrap.servers": 'kafka:29092',  # Single string, not a list
        "auto.offset.reset": 'latest',
        "group.id": 'FmConsumerGroup-1',
        "enable.auto.commit": False
    },

    "dest_Connections": {
        'host': '10.10.25.46',
        'user': 'foc_user',
        'password': 'Foc_user@#4321',
        'database': 'foc_db',
        'port': 3306
    },

     "source_connection": {
        'host': '10.10.25.130',
        'user': 'log_stream',
        'password': '10g236!shdhjaF',
        'database': 'tracking_live',
        'port': 3307
    },

    "producer_settings": {
        "bootstrap.servers": 'kafka:29092'  # Same as consumer settings
    },

    "topics": ['tracking_live.tracking_live.BOOKING_DETAILS','tracking_live.tracking_live.CONSIGNMENT','tracking_live.tracking_live.PAYMENT_DETAILS','tracking_live.tracking_live.ADDRESS_DETAILS','tracking_live.tracking_live.CONSIGNMENT_HISTORY'],

    "Consignment_Final_Schema": ["foc_cnno", "foc_booking_date", "foc_bkoffice_id", "foc_bkdate_id", "foc_bkservice_code", "foc_bkmode_code", "foc_bkoffice_code", "foc_bkdate", "foc_edd", "foc_bkgpin_code", "foc_reddsla_date", "foc_customer_promise_edd", "foc_softdata_pickup_refno", "foc_cust_ref_no", "foc_pickup_customer_code", "foc_edd_booked_status", "foc_bkserv_code", "foc_bkpin_code"],

    "Booking_Details_Final_Schema": ['foc_cnno', 'foc_cpdp_code', 'record_updated_date','foc_bkcusttype_code','foc_bkcust_code','foc_bookchannel','foc_bkdocument_code','foc_Booked_Wt','foc_Vol_Wt','foc_Billed_Wt','foc_pcs','foc_nop','foc_booking_app', 'foc_content_desc'],

    "Adress_Details_Final_Schema": ['foc_cnno','record_updated_date','foc_receiver_name','foc_receiver_phno', 'foc_receiver_address', 'foc_receiver_city', 'foc_receiver_state', 'foc_receiver_pincode', 'foc_sender_name', 'foc_sender_phno', 'foc_sender_address', 'foc_sender_city', 'foc_sender_state', 'foc_sender_pincode', 'foc_receiver_company_name', 'foc_sender_company_name'],

    "Payment_Details_Final_Schema": ['foc_cnno','record_updated_date', 'foc_cod_flag', 'foc_bkinvvalue'],
    
    "Consignment_History_Final_Schema": ['foc_cnno', 'record_updated_date', "foc_pickup_date", "foc_pickup_booked_by", "foc_pickup_remarks", "foc_pickup_scheduled_date", "foc_pickup_cancelled_date", "foc_pickup_branch_code", "foc_pickup_branch_name", "foc_pickup_awaited_date", "foc_picked_up_date", "foc_dropoff_awaited_date", "foc_dropoff_scheduled_date", "foc_dropoff_completed_date", "foc_dropoff_cancelled_date", "foc_dropoff_rejected_date", "foc_pickup_hub_type", "foc_pickup_city_code", "foc_pre_bkpmfmadefor_loccode",'foc_pickup_attempt_date', 'foc_pickup_reassigned_date', 'foc_pickup_attempt2_date', 'foc_pickup_attempt3_date', 'foc_pickup_attempt3_date', 'foc_pickup_attemptl_date', 'foc_pickup_scheduled2_date', 'foc_pickup_scheduled3_date', 'foc_pickup_scheduledl_date', 'foc_pickup_reassigned2_date', 'foc_pickup_reassigned3_date', 'foc_pickup_reassignedl_date', 'foc_pickup2_remarks', 'foc_pickup3_remarks', 'foc_pickupl_remarks', 'foc_pickup_npr_reason', 'foc_pickup_npr2_reason', 'foc_pickup_npr3_reason', 'foc_pickup_nprl_reason', 'foc_pickup_cancelled2_date', 'foc_pickup_cancelled3_date', 'foc_pickup_cancelledl_date', 'foc_pre_bkbrpmfno']                                 
}
