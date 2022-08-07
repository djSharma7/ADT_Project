import pandas as pd
import DatabaseOperations as DB
import SqsOperations as SQS
import Crawler as CR
import OutputWriter as OW

group_id = 1



def update_group_input(DB_OBJ,SQS_OBJ):
    '''
    Read input sheet.
    Update the keyword input for the group in the database
    '''
    df = pd.ExcelFile('Input Sheet.xlsx')
    print("**Read Client Input")
    input_json = []
    keywords = df.sheet_names or []
    for keyword in keywords:
        try:
            k_df = df.parse(sheet_name=keyword)
            for i in k_df.index:
                obj = {}
                self_sku = k_df.iloc[i]['SELF SKU']
                comp_sku = k_df.iloc[i]['COMPETITOR SKU']

                if type(self_sku) != float:
                    obj['keyword'] = keyword.strip()
                    obj['client_id'] = 1
                    obj['sku'] = self_sku.strip()
                    obj['is_competitor'] = False
                    input_json.append(obj.copy())
                if type(comp_sku) != float:
                    obj['keyword'] = keyword.strip()
                    obj['sku'] = comp_sku.strip()
                    obj['client_id'] = 1
                    obj['is_competitor'] = True
                    input_json.append(obj.copy())
        except Exception as e:
            print ("Exception keyword input : ",e)
            pass
    '''
    Updating the keyword input in the database
    and
    Fetch the updated input data for crawling
    '''
    print ("**Input Length:- ",len(input_json))
    messages = DB_OBJ.update_and_fetch_input(input_json)
    for message in messages:
        try:
            message['current_page'] = 1
            response = SQS_OBJ.insert_message_into_queue(message)
            if response:
                print ("**SQS Message Written Successfully.")
            else:
                print (response,"not written")
        except Exception as e:
            print ("Exception in sending message to sqs :-",e)
            pass


if __name__ =='__main__':
    DB_OBJ = DB.DatabaseOperations()
    SQS_OBJ = SQS.SqsOperations()
    print ("**DB & SQS Initialised.")

    ###META INFO
    ##client_id = 1
    update_group_input(DB_OBJ,SQS_OBJ)
    CR.Crawler(SQS_OBJ,DB_OBJ)
    print ("**Crawling Done!")

    OW.OutputWriter(DB_OBJ)
    print ("**Results Written In Excel In Results Folder")
    print("**DONE!!")











