import logging
import json
import pandas as pd
import numpy as np
from confluent_kafka import Consumer, KafkaError
from datetime import datetime
import mysql.connector
from contextlib import closing
import Fm_transformations as tc
from contextlib import closing

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class KafkaConsumer:
    def __init__(self, consumer_settings, consumer_topic, dest_settings, source_connection,
                 Consignment_Final_Schema, Booking_Details_Final_Schema,
                 Payment_Details_Final_Schema, Adress_Details_Final_Schema,
                 Consignment_History_Final_Schema):
        self.consumer_settings = consumer_settings
        self.now = datetime.now()
        self.curent_dt = self.now.strftime("%Y-%m-%d %H:%M:%S")
        self.dest_settings = dest_settings
        self.source_settings = source_connection
        self.Consignment_Final_Schema = Consignment_Final_Schema
        self.Booking_Details_Final_Schema = Booking_Details_Final_Schema
        self.Payment_Details_Final_Schema = Payment_Details_Final_Schema
        self.Adress_Details_Final_Schema = Adress_Details_Final_Schema
        self.Consignment_History_Final_Schema = Consignment_History_Final_Schema
        self.consumer_topic = consumer_topic
        self.foc_consigments = 'fm_foc_consignments'
        self.consumer_instance = None

        self.transform_consignments = tc.transform_consignments
        self.transform_address_dtails = tc.transform_address_dtails
        self.transform_booking_details = tc.transform_booking_details
        self.transform_payment_details = tc.transform_payment_details
        self.Transform_Consignment_History = tc.Transform_Consignment_History

    def create_consumer(self):
        self.consumer_instance = Consumer(**self.consumer_settings)

    def subscribe(self):
        if self.consumer_instance is None:
            raise Exception("Consumer instance is not created. Call create_consumer() first.")
        self.consumer_instance.subscribe(self.consumer_topic)

    def consume_messages(self):
        if self.consumer_instance is None:
            raise Exception("Consumer instance is not created. Call create_consumer() first.")
        #consumer_id = self.consumer_instance.config['client.id']
        #logging.info(f"Starting consumer with ID: {consumer_id}")
        try:
            while True:
                msg = self.consumer_instance.poll(0)  # Adjusted polling time
                if msg is None:
                    continue
                if msg.error():
                    self.handle_error(msg.error())
                    continue  # Continue consuming after handling the error
                message_value = msg.value().decode('utf-8')
                tracking_number = self.extract_tracking_number(message_value)
                table_name = self.extract_table_name(message_value)

                if tracking_number and table_name:
                    transformed_data = self.transform_data(message_value, table_name)
                if transformed_data is not None:
                    self.process_message(table_name, transformed_data, message_value)

        except KeyboardInterrupt:
            logging.info("Consumer interrupted.")
        except Exception as e:
            logging.error(f"An error occurred during message consumption: {e}")
        finally:
            self.close_consumer()

    def handle_error(self, error):
        if error.code() == KafkaError._PARTITION_EOF:
            return  # End of partition, nothing to do
        logging.error(f"Consumer error: {error}")

    def process_message(self, table_name, transformed_data, message_value):
        if table_name == 'CONSIGNMENT':
            queries = self.generate_upsert_query(self.foc_consigments, transformed_data, primary_key='id')
            self.execute_upsert_queries(queries, self.dest_settings)
            self.consignment_custom_columns(self.foc_consigments, transformed_data)

        elif table_name == 'CONSIGNMENT_HISTORY':
            self.history_table_custom_updates(self.foc_consigments,table_name, transformed_data, message_value)
            #self.execute_update_query(self.foc_consigments, table_name, transformed_data, message_value)
        
        elif table_name == 'BOOKING_DETAILS':
            self.booking_details_updates(self.foc_consigments,table_name, transformed_data, message_value)

        elif table_name == 'PAYMENT_DETAILS':
            self.payments_details_updates(self.foc_consigments,table_name, transformed_data, message_value)

        else:
            pass    


    def close_consumer(self):
        if self.consumer_instance:
            self.consumer_instance.close()
            logging.info("Consumer closed.")

    def extract_tracking_number(self, message_value):
        try:
            data = json.loads(message_value)
            return data['after'].get("Tracking_Number")
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(f"Failed to extract Tracking_Number: {e}")
            return None

    def extract_table_name(self, message_value):
        try:
            data = json.loads(message_value)
            return data['source']['table']
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(f"Failed to extract table: {e}")
            return None

    def transform_data(self, message_value, topic):
        try:
            data = json.loads(message_value)
            opType = data['op']
            if opType not in ['c', 'u']:
                logging.warning(f"Unsupported operation type: {opType} for topic: {topic}")
                return None

            df = pd.DataFrame([data['after']])

            transformations = {
                'CONSIGNMENT': (self.transform_consignments, self.Consignment_Final_Schema),
                'BOOKING_DETAILS': (self.transform_booking_details, self.Booking_Details_Final_Schema),
                'PAYMENT_DETAILS': (self.transform_payment_details, self.Payment_Details_Final_Schema),
                #'ADDRESS_DETAILS': (self.transform_address_dtails, self.Adress_Details_Final_Schema),
                'CONSIGNMENT_HISTORY': (self.Transform_Consignment_History, self.Consignment_History_Final_Schema)
            }

            if topic in transformations:
                transform_func, schema = transformations[topic]
                latest_update_df = transform_func(self,df)
                latest_update_df = latest_update_df[schema]
                logging.info(f"{topic} transformed successfully with operation type: {opType}.")
                return latest_update_df

            logging.warning(f"Unsupported topic: {topic}")
            return None

        except Exception as e:
            logging.error(f"Transformation failed for topic: {topic} with error: {e}")
        return None

    def generate_upsert_query(self, table_name, df, primary_key):
        foc_pickup_customer_code = df.loc[0, 'foc_pickup_customer_code']
        queries = []
        df = df.drop('foc_pickup_customer_code', axis = 1)
        for _, row in df.iterrows():
                columns = ', '.join(row.index)
                values = ', '.join(f"'{str(value).replace('\'', '\'\'')}'" for value in row.values)
                update_columns = ', '.join(f"{col} = VALUES({col})" for col in row.index if col != primary_key)
                query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({values})
                ON DUPLICATE KEY UPDATE {update_columns};"""
                queries.append(query.strip())

        return queries

    def execute_upsert_queries(self, queries, db_config):
        try:
            with closing(mysql.connector.connect(**db_config)) as connection:
                with closing(connection.cursor()) as cursor:
                    for query in queries:
                        cursor.execute(query)

                    connection.commit()
                    logging.info("All upsert queries executed successfully.")

        except mysql.connector.Error as err:
            logging.error(f"SQL Error: {err}")

    def generate_insert_queries(self, foc_consigments_tbl, table_name, df, message_value):
        queries = []
        columns = df.columns.tolist()
        for index, row in df.iterrows():
            values = ', '.join(f"'{str(value).replace('\'', '\'\'')}'" if pd.notnull(value) else 'NULL' for value in row)
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values});"
            queries.append(query)
        return queries    

    def consignment_custom_columns(self, foc_consigments_tbl, df) :
         foc_cnno = df.loc[0, 'foc_cnno']
         foc_pickup_customer_code = df.loc[0, 'foc_pickup_customer_code']
         try:
            with closing(mysql.connector.connect(**self.dest_settings)) as connection:
                with closing(connection.cursor()) as cursor:
                   if foc_pickup_customer_code is not None:
                    consignment_sql = f"""update {foc_consigments_tbl} 
                                          set foc_pickup_customer_code = '{foc_pickup_customer_code}' 
                                          where foc_cnno = '{foc_cnno}';"""
                    cursor.execute(consignment_sql)
                    connection.commit()
                    logging.info(f"Pickup Customer Foc: {foc_cnno} Code {foc_pickup_customer_code} Update successfully.")
                   else:
                       logging.info("foc_pickup_customer_code is none.") 

         except mysql.connector.Error as err:
            logging.error(f"SQL Error: {err}") 


    def history_table_custom_updates(self, foc_consigments_tbl, table_name, df, message_value):
        try:
            source_connection = mysql.connector.connect(**self.source_settings)
            source_cursor = source_connection.cursor(dictionary=True)
            dest_connection = mysql.connector.connect(**self.dest_settings)
            dest_cursor = dest_connection.cursor()
            rows_updated = 0
            foc_cnno = df.loc[0 ,'foc_cnno']

            foc_pickup_date = df.loc[0 ,'foc_pickup_date']
            if foc_pickup_date is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pickup_date = '{foc_pickup_date}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount

            foc_pickup_booked_by = df.loc[0 ,'foc_pickup_booked_by']
            if foc_pickup_booked_by is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pickup_booked_by = '{foc_pickup_booked_by}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount

            foc_pickup_remarks = df.loc[0 ,'foc_pickup_remarks']  
            if  foc_pickup_remarks is not None:
                select_sql = f"""select foc_pickup_remarks from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_remarks is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_remarks =  '{foc_pickup_remarks}' , 
                                       foc_pickupl_remarks = '{foc_pickup_remarks}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount

            foc_pickup_scheduled_date = df.loc[0 ,'foc_pickup_scheduled_date']
            if foc_pickup_scheduled_date is not None:
                select_sql = f"""select foc_pickup_scheduled_date from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_scheduled_date is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_scheduled_date = '{foc_pickup_scheduled_date}' , 
                                       foc_pickup_scheduledl_date = '{foc_pickup_scheduled_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount

            foc_pickup_cancelled_date = df.loc[0 ,'foc_pickup_cancelled_date']
            if foc_pickup_cancelled_date is not None:
                select_sql = f"""select foc_pickup_cancelled_date from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_cancelled_date is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_cancelled_date =  '{foc_pickup_cancelled_date}' , 
                                       foc_pickup_cancelledl_date = '{foc_pickup_cancelled_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount  

            foc_pickup_branch_code = df.loc[0 ,'foc_pickup_branch_code'] 
            if foc_pickup_branch_code is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pickup_branch_code =  '{foc_pickup_branch_code}'  
                                   where foc_cnno = '{foc_cnno}';"""
                print(update_query)
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount        

            foc_pickup_branch_name = df.loc[0 ,'foc_pickup_branch_name'] 
            if foc_pickup_branch_name is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pickup_branch_name =  '{foc_pickup_branch_name}'  
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 

            foc_pickup_awaited_date = df.loc[0 ,'foc_pickup_awaited_date'] 
            if foc_pickup_awaited_date is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pickup_awaited_date =  '{foc_pickup_awaited_date}'  
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount

            foc_picked_up_date = df.loc[0 ,'foc_picked_up_date'] 
            if foc_picked_up_date is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_picked_up_date =  '{foc_picked_up_date}'  
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount


            foc_dropoff_awaited_date = df.loc[0 ,'foc_dropoff_awaited_date'] 
            if foc_dropoff_awaited_date is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dropoff_awaited_date =  '{foc_dropoff_awaited_date}'  
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 


            foc_dropoff_scheduled_date = df.loc[0 ,'foc_dropoff_scheduled_date'] 
            if foc_dropoff_scheduled_date is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dropoff_scheduled_date =  '{foc_dropoff_scheduled_date}'  
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount  

            foc_dropoff_completed_date = df.loc[0 ,'foc_dropoff_completed_date'] 
            if foc_dropoff_completed_date is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dropoff_completed_date =  '{foc_dropoff_completed_date}'  
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 

            foc_dropoff_cancelled_date = df.loc[0 ,'foc_dropoff_cancelled_date'] 
            if foc_dropoff_cancelled_date is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dropoff_cancelled_date =  '{foc_dropoff_cancelled_date}'  
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount
                
            foc_dropoff_rejected_date = df.loc[0 ,'foc_dropoff_rejected_date'] 
            if foc_dropoff_rejected_date is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dropoff_rejected_date =  '{foc_dropoff_rejected_date}'  
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount

            foc_pickup_hub_type = df.loc[0 ,'foc_pickup_hub_type'] 
            if foc_pickup_hub_type is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pickup_hub_type =  '{foc_pickup_hub_type}'  
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount  


            foc_pickup_city_code = df.loc[0 ,'foc_pickup_city_code'] 
            if foc_pickup_city_code is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pickup_city_code =  '{foc_pickup_city_code}'  
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount

            foc_pre_bkpmfmadefor_loccode = df.loc[0 ,'foc_pre_bkpmfmadefor_loccode'] 
            if foc_pre_bkpmfmadefor_loccode is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pre_bkpmfmadefor_loccode =  '{foc_pre_bkpmfmadefor_loccode}'  
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount   


            foc_pickup_attempt_date = df.loc[0 ,'foc_pickup_attempt_date']
            if foc_pickup_attempt_date is not None:
                select_sql = f"""select foc_pickup_attempt_date from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_attempt_date is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_attempt_date =  '{foc_pickup_attempt_date}' , 
                                       foc_pickup_attemptl_date = '{foc_pickup_attempt_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount 

            foc_pickup_reassigned_date = df.loc[0 ,'foc_pickup_reassigned_date']
            if foc_pickup_reassigned_date is not None:
                select_sql = f"""select foc_pickup_reassigned_date from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_reassigned_date is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_reassigned_date =  '{foc_pickup_reassigned_date}' , 
                                       foc_pickup_reassignedl_date = '{foc_pickup_reassigned_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount  

            foc_pickup_reassignedl_date = df.loc[0 ,'foc_pickup_reassignedl_date'] 
            if foc_pickup_reassignedl_date is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pickup_reassignedl_date =  '{foc_pickup_reassignedl_date}'  
                                   where foc_cnno = '{foc_cnno}';"""
                print(update_query)
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 

            foc_pickup_attempt2_date = df.loc[0 ,'foc_pickup_attempt2_date']
            if foc_pickup_attempt2_date is not None:
                select_sql = f"""select foc_pickup_attempt2_date from fm_foc_consignments where foc_cnno = '{foc_cnno}'
                                 and foc_pickup_attempt_date is not null  
                                 and foc_pickup_attempt2_date is null 
                                 and foc_pickup_attempt_date != '{foc_pickup_attempt2_date}' ;"""  
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_attempt2_date =  '{foc_pickup_attempt2_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount  

            foc_pickup_attempt3_date = df.loc[0 ,'foc_pickup_attempt3_date']
            if foc_pickup_attempt3_date is not None:
                select_sql = f"""select foc_pickup_attempt3_date from fm_foc_consignments where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_attempt2_date is not null  
                                 and foc_pickup_attempt3_date is null 
                                 and foc_pickup_attempt2_date != '{foc_pickup_attempt3_date}' ;"""  
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_attempt3_date =  '{foc_pickup_attempt3_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount 

            foc_pickup_attemptl_date = df.loc[0 ,'foc_pickup_attemptl_date'] 
            if foc_pickup_attemptl_date is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pickup_attemptl_date =  '{foc_pickup_attemptl_date}'  
                                   where foc_cnno = '{foc_cnno}';"""
                print(update_query)
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 


            foc_pickup_scheduledl_date = df.loc[0 ,'foc_pickup_scheduledl_date'] 
            if foc_pickup_scheduledl_date is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pickup_scheduledl_date =  '{foc_pickup_scheduledl_date}'  
                                   where foc_cnno = '{foc_cnno}';"""
                print(update_query)
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 


            foc_pickupl_remarks = df.loc[0 ,'foc_pickupl_remarks'] 
            if foc_pickupl_remarks is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pickupl_remarks =  '{foc_pickupl_remarks}'  
                                   where foc_cnno = '{foc_cnno}';"""
                print(update_query)
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount  

            foc_pickup_nprl_reason = df.loc[0 ,'foc_pickup_nprl_reason'] 
            if foc_pickup_nprl_reason is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pickup_nprl_reason =  '{foc_pickup_nprl_reason}'  
                                   where foc_cnno = '{foc_cnno}';"""
                print(update_query)
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount

            foc_pickup_cancelledl_date = df.loc[0 ,'foc_pickup_cancelledl_date'] 
            if foc_pickup_cancelledl_date is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pickup_cancelledl_date =  '{foc_pickup_cancelledl_date}'  
                                   where foc_cnno = '{foc_cnno}';"""
                print(update_query)
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount  


            foc_pickup_scheduled2_date = df.loc[0 ,'foc_pickup_scheduled2_date']
            if foc_pickup_scheduled2_date is not None:
                select_sql = f"""select foc_pickup_scheduled2_date from fm_foc_consignments where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_scheduled_date is not null  
                                 and foc_pickup_scheduled2_date is null 
                                 and foc_pickup_scheduled_date != '{foc_pickup_scheduled2_date}' ;"""  
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_scheduled2_date =  '{foc_pickup_scheduled2_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount    

            foc_pickup_scheduled3_date = df.loc[0 ,'foc_pickup_scheduled3_date']
            if foc_pickup_scheduled3_date is not None:
                select_sql = f"""select foc_pickup_scheduled3_date from fm_foc_consignments 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_scheduled2_date is not null  
                                 and foc_pickup_scheduled3_date is null 
                                 and foc_pickup_scheduled2_date != '{foc_pickup_scheduled3_date}' ;"""  
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_scheduled3_date =  '{foc_pickup_scheduled3_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount      


            foc_pickup_reassigned2_date = df.loc[0 ,'foc_pickup_reassigned2_date']
            if foc_pickup_reassigned2_date is not None:
                select_sql = f"""select foc_pickup_reassigned2_date from fm_foc_consignments 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_reassigned_date is not null  
                                 and foc_pickup_reassigned2_date is null 
                                 and foc_pickup_reassigned_date != '{foc_pickup_reassigned2_date}' ;"""  
                dest_cursor.execute(select_sql)

                row_value = dest_cursor.fetchall()
                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_reassigned2_date =  '{foc_pickup_reassigned2_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount

            foc_pickup_reassigned3_date = df.loc[0 ,'foc_pickup_reassigned3_date']
            if foc_pickup_reassigned3_date is not None:
                select_sql = f"""select foc_pickup_reassigned3_date from fm_foc_consignments 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_reassigned2_date is not null  
                                 and foc_pickup_reassigned3_date is null 
                                 and foc_pickup_reassigned2_date != '{foc_pickup_reassigned3_date}' ;"""  
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_reassigned3_date =  '{foc_pickup_reassigned3_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount  


            foc_pickup2_remarks = df.loc[0 ,'foc_pickup2_remarks']
            if foc_pickup2_remarks is not None:
                select_sql = f"""select foc_pickup2_remarks from fm_foc_consignments 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_remarks is not null  
                                 and foc_pickup2_remarks is null 
                                 and foc_pickup_attempt_date != '{foc_pickup_attempt2_date}' ;"""  
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup2_remarks =  '{foc_pickup2_remarks}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount 

                    
            foc_pickup3_remarks = df.loc[0 ,'foc_pickup3_remarks']
            if foc_pickup3_remarks is not None:
                select_sql = f"""select foc_pickup3_remarks from fm_foc_consignments 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup2_remarks is not null  
                                 and foc_pickup3_remarks is null 
                                 and foc_pickup_attempt2_date != '{foc_pickup_attempt3_date}' ;"""  
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup3_remarks =  '{foc_pickup3_remarks}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount 


            foc_pickup_npr_reason = df.loc[0 ,'foc_pickup_npr_reason']
            if foc_pickup_npr_reason is not None:
                select_sql = f"""select foc_pickup_npr_reason from fm_foc_consignments
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_attempt_date is null"""  
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_npr_reason =  '{foc_pickup_npr_reason}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount  


            foc_pickup_npr2_reason = df.loc[0 ,'foc_pickup_npr2_reason']
            if foc_pickup_npr2_reason is not None:
                select_sql = f"""select foc_pickup_npr2_reason from fm_foc_consignments 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_npr_reason is not null  
                                 and foc_pickup_npr2_reason is null 
                                 and foc_pickup_attempt_date != '{foc_pickup_attempt2_date}' ;"""  
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_npr2_reason =  '{foc_pickup_npr2_reason}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount

            foc_pickup_npr3_reason = df.loc[0 ,'foc_pickup_npr3_reason']
            if foc_pickup_npr3_reason is not None:
                select_sql = f"""select foc_pickup_npr3_reason from fm_foc_consignments 
                                 where foc_cnno = '{foc_cnno}' and
                                 foc_pickup_npr2_reason is not null  
                                 and foc_pickup_npr3_reason is null 
                                 and foc_pickup_attempt2_date != '{foc_pickup_attempt3_date}' ;"""  
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_npr3_reason =  '{foc_pickup_npr2_reason}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount  


            foc_pickup_cancelled2_date = df.loc[0 ,'foc_pickup_cancelled2_date']
            if foc_pickup_cancelled2_date is not None:
                select_sql = f"""select foc_pickup_cancelled2_date from fm_foc_consignments 
                                 where foc_cnno = '{foc_cnno}' and
                                 foc_pickup_cancelled_date is not null  
                                 and foc_pickup_cancelled2_date is null 
                                 and foc_pickup_cancelled_date != '{foc_pickup_cancelled2_date}' ;"""  
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()
                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_cancelled2_date =  '{foc_pickup_cancelled2_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount


            foc_pickup_cancelled3_date = df.loc[0 ,'foc_pickup_cancelled3_date']
            if foc_pickup_cancelled3_date is not None:
                select_sql = f"""select foc_pickup_cancelled3_date from fm_foc_consignments 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_pickup_cancelled2_date is not null  
                                 and foc_pickup_cancelled3_date is null 
                                 and foc_pickup_cancelled2_date != '{foc_pickup_cancelled3_date}' ;"""  
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pickup_cancelled3_date =  '{foc_pickup_cancelled3_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    print(update_query)
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount 


            foc_pre_bkbrpmfno = df.loc[0 ,'foc_pre_bkbrpmfno'] 
            if foc_pre_bkbrpmfno is not None:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_pre_bkbrpmfno =  '{foc_pre_bkbrpmfno}'  
                                   where foc_cnno = '{foc_cnno}';"""
                print(update_query)
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount                                                                                                                                                                   


            if rows_updated > 0:
                dest_connection.commit()
                logging.info(f"Successfully updated {rows_updated} row(s) for foc_cnno {foc_cnno}.") 
            else:
                 self.log_zero_update(table_name, message_value)
                 logging.info(f"No rows updated for foc_cnno {foc_cnno}.")             

        except mysql.connector.Error as err:
            # Log MySQL specific errors
            logging.error(f"MySQL error: {err}")
        except Exception as e:
            # Log any other unexpected errors
            logging.error(f"Unexpected error: {e}")
        finally:
            for cursor in [source_cursor, dest_cursor]:
                if cursor:
                    cursor.close()
            for connection in [source_connection, dest_connection]:
                if connection:
                    connection.close()  


    def booking_details_updates(self, foc_consigments_tbl, table_name, df, message_value):
        try:
            with closing(mysql.connector.connect(**self.dest_settings)) as connection:
                with closing(connection.cursor()) as cursor:
                    rows_updated = 0 
                    for _, row in df.iterrows():
                        set_clause = ', '.join(f"{col} = %s" for col in row.index if col != 'foc_cnno')
                        update_query = f"""UPDATE {foc_consigments_tbl} SET {set_clause} WHERE foc_cnno = %s;"""
                        values = (*row.drop('foc_cnno').values, row['foc_cnno'])
                        cursor.execute(update_query, values)
                        rows_updated += cursor.rowcount  # Increment the count of updated rows

                    if rows_updated == 0:
                        self.log_zero_update(table_name, message_value)

                    connection.commit()
                    logging.info("All Booking Detailes queries executed successfully.")

        except mysql.connector.Error as err:
            logging.error(f"SQL Error: {err}")


    def payments_details_updates(self, foc_consigments_tbl, table_name, df, message_value):
        try:
            with closing(mysql.connector.connect(**self.dest_settings)) as connection:
                with closing(connection.cursor()) as cursor:
                    rows_updated = 0 
                    for _, row in df.iterrows():
                        set_clause = ', '.join(f"{col} = %s" for col in row.index if col != 'foc_cnno')
                        update_query = f"""UPDATE {foc_consigments_tbl} SET {set_clause} WHERE foc_cnno = %s;"""
                        values = (*row.drop('foc_cnno').values, row['foc_cnno'])
                        cursor.execute(update_query, values)
                        rows_updated += cursor.rowcount  # Increment the count of updated rows

                    if rows_updated == 0:
                        self.log_zero_update(table_name, message_value)

                    connection.commit()
                    logging.info("All Payments Detailes queries executed successfully.")

        except mysql.connector.Error as err:
            logging.error(f"SQL Error: {err}")                                


    def execute_update_query(self, foc_consigments_tbl, table_name, df, message_value):
        try:
            with closing(mysql.connector.connect(**self.dest_settings)) as connection:
                with closing(connection.cursor()) as cursor:
                    rows_updated = 0  # Track the number of rows updated
                    if table_name in ['PAYMENT_DETAILS', 'ADDRESS_DETAILS', 'BOOKING_DETAILS']:
                        for _, row in df.iterrows():
                            set_clause = ', '.join(f"{col} = %s" for col in row.index if col != 'foc_cnno')
                            update_query = f"""UPDATE {foc_consigments_tbl} SET {set_clause} WHERE foc_cnno = %s;"""
                            values = (*row.drop('foc_cnno').values, row['foc_cnno'])
                            cursor.execute(update_query, values)
                            rows_updated += cursor.rowcount  # Increment the count of updated rows
                    else:
                        for _, row in df.iterrows():
                            foc_delvstatus_code = row['foc_non_dlv_attempts']
                            foc_ch_dlv_attempts = row['foc_rto_non_dlv_attempts']
                            foc_cnno = row['foc_cnno']
                            sql = f"""UPDATE {foc_consigments_tbl} SET foc_non_dlv_attempts = foc_non_dlv_attempts + %s, foc_rto_non_dlv_attempts = foc_rto_non_dlv_attempts + %s WHERE foc_cnno = %s;"""
                            values = (foc_delvstatus_code, foc_ch_dlv_attempts, foc_cnno)
                            cursor.execute(sql, values)
                            rows_updated += cursor.rowcount  # Increment the count of updated rows

                    if rows_updated == 0:
                        self.log_zero_update(table_name, message_value)

                    connection.commit()
                    logging.info("All update queries executed successfully.")

        except mysql.connector.Error as err:
            logging.error(f"SQL Error: {err}")

    def log_zero_update(self, table_name, message_value):
        try:
            with closing(mysql.connector.connect(**self.dest_settings)) as connection:
                with closing(connection.cursor()) as cursor:
                    log_query = "INSERT INTO failure_log (table_name, message) VALUES (%s, %s);"
                    cursor.execute(log_query, (table_name, message_value))
                    connection.commit()
                    logging.info(f"Logged zero update for message in {table_name}.")
        except mysql.connector.Error as err:
            logging.error(f"Failed to log zero update: {err}")

    def run(self):
        try:
            self.create_consumer()
            self.subscribe()
            self.consume_messages()
        except Exception as e:
            logging.error(f"An error occurred in run: {e}")
        finally:
            self.close_consumer()
