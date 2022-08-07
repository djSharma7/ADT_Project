import psycopg2
import json

class DatabaseOperations:

    def __init__(self):
        self.connect = psycopg2.connect(host="localhost", database="ADT", user="postgres", password="deepanshu7*")
        self.conn = self.connect
        self.group_id = 1
        return


    def update_and_fetch_input(self,raw_input):
        # try:
        #     print (json.dumps(raw_input))
        #     exit ()
        #     cur = self.conn.cursor()
        #     query_str = '''
        #         DROP TABLE IF EXISTS new_input;
        #         CREATE TEMP TABLE new_input  (
        #             keyword text,
        #             sku text,
        #             type text
        #         );
        #
        #         INSERT INTO new_input
        #         select * from json_populate_recordset(null::new_input,
        #                                   '{json_input}');
        #
        #         DROP TABLE IF EXISTS new_keyword_input;
        #         CREATE TEMP TABLE new_keyword_input as (
        #             SELECT DISTINCT keyword FROM new_input
        #         );
        #
        #         DROP TABLE IF EXISTS  current_keyword_input;
        #         CREATE TEMP TABLE current_keyword_input as (
        #             SELECT keyword,kk.id
        #             FROM "ADT_Master"."Keyword" as kk
        #         );
        #
        #         INSERT INTO "ADT_Master"."Keyword" (keyword)
        #         SELECT nki.keyword FROM new_keyword_input as nki
        #             LEFT JOIN current_keyword_input as cki
        #                 ON cki.keyword= nki.keyword
        #                 where cki.keyword is null;
        #
        #
        #         INSERT INTO "ADT_Master"."Client_Keyword_Mapping" (client_id,keyword_id)
        #         SELECT 1,id FROM "ADT_Master"."Keyword" as amk
        #             LEFT JOIN  new_keyword_input as nki
        #             ON amk.keyword = nki.keyword
        #              ON CONFLICT DO NOTHING;
        #
        #
        #         INSERT INTO "ADT_Master"."Client_Keyword_Sku_Mapping" (client_id,keyword_id,sku,is_comp)
        #         SELECT 1,amk.id,ni.sku,
        #             CASE
        #                 WHEN ni.type ='self'
        #                 THEN true
        #             ELSE false
        #             END
        #         FROM "ADT_Master"."Keyword" as amk
        #         INNER JOIN new_input as ni
        #         on ni.keyword = amk.keyword
        #         ON CONFLICT DO NOTHING;
        #
        #         SELECT true;
        #         '''.format(json_input=json.dumps(raw_input))
        #     cur.execute(query_str)
        #     print ()
        #     print (query_str)
        #     print()
        #     status = cur.fetchone()[0]
        # except Exception as e:
        #     print ("Exception update keyword input:- ",e)
        #     return False
        # return True
        try:
            cur = self.conn.cursor()
            # print (json.dumps(raw_input))
            cur.callproc('"ADT_Master".update_and_fetch_client_input', [json.dumps(raw_input)])
            res =cur.fetchone()[0]
            self.connection_commit()
            return json.loads(json.dumps(res))
        except Exception as e:
            print ("Exception DB Operations update_keyword_input:- ", e)
            return []

    def connection_commit(self):
        self.conn.commit()

    def insert_crawled_records(self,output):
        # try:
        #     output = output.replace("'","")
        #
        #     cur = self.conn.cursor()
        #     query_str = '''
        #             DROP TABLE IF EXISTS crawled_output;
        #             CREATE TEMP TABLE crawled_output  (
        #                 keyword_id Integer,
        #                 is_active boolean,
        #                 product_id text,
        #                 product_name text,
        #                 product_price double precision,
        #                 product_rank integer,
        #                 product_image_url text,
        #                 is_sponsored boolean,
        #                 product_url text,
        #                 product_review_count integer,
        #                 product_rating double precision
        #             );
        #
        #             INSERT INTO crawled_output
        #             select * from json_populate_recordset(null::crawled_output,
        #                                                   '{output}');
        #
        #
        #             UPDATE "ADT_Master"."Crawled_Keyword_Data"
        #                                                   SET is_active = false;
        #
        #             INSERT INTO "ADT_Master"."Crawled_Keyword_Data"
        #                 (
        #                 crawl_date,
        #                  creation_timestamp,
        #                  keyword_id,
        #                  is_active ,
        #                 product_id ,
        #                 product_name ,
        #                 product_price,
        #                 product_rank,
        #                 product_image_url,
        #                 is_sponsored,
        #                 product_url,
        #                 product_review_count,
        #                 product_rating)
        #
        #             SELECT Current_Date,current_timestamp,crawled_output.*
        #                                                   FROM crawled_output;
        #                                                   '''.format(output=output)
        #     cur.execute(query_str)
        #
        #     print()
        #     print(query_str)
        #     print()
        # except Exception as e :
        #     print ("Exception in Insert Record:- ",e)
        #     print ()
        #     print (output)
        # return
        try:
            cur = self.conn.cursor()
            cur.callproc('"ADT_Master".insert_crawled_keyword_data', [output])
            res = cur.fetchone()[0]
            self.connection_commit()
            return res
        except Exception as e:
            print ("Exception DB operation insert_crawled_records:- ",e)

    def get_historical_keyword_data(self,number_of_days=10):
        try:
            client_id = 1
            cur = self.conn.cursor()
            cur.callproc('"ADT_Master".get_historical_keyword_data',[number_of_days,client_id])
            res = cur.fetchone()[0]
            self.connection_commit()
            return res
        except Exception as e:
            print ("Exception DB operation get_historical_keyword_data:- ",e)
            return []


    # def get_output_results_from_db(self,historical_days =30):
    #     messages = []
    #     try:
    #         cur = self.conn.cursor()
    #         query_str = '''SELECT
    #
    #                       JSON_AGG(
    #                           JSON_BUILD_OBJECT(
    #                            'KEYWORD',t.keyword,
    #                            'CRAWL DATE',t.crawl_date,
    #                             'SKU', t.sku,
    #                             'SKU TYPE', t.sku_type,
    #                             'PRODUCT RANK',t.product_rank,
    #                             'PRODUCT NAME',t.product_name)
    #                          )
    #                 FROM (
    #                 SELECT
    #
    #                     kk.keyword as keyword,
    #                     ckd.crawl_date as crawl_date,
    #                     ckd.product_id as sku,
    #                      CASE WHEN cksm.is_comp is true
    #                      THEN 'Competitor'
    #                      ELSE 'Self'
    #                      END as sku_type,
    #                     ckd.product_rank,
    #                     ckd.product_name
    #
    #                    FROM "ADT_Master"."Crawled_Keyword_Data" as ckd
    #                    INNER JOIN "ADT_Master"."Keyword" as kk
    #                     ON kk.id = ckd.keyword_id
    #
    #                  INNER JOIN
    #                     "ADT_Master"."Client_Keyword_Sku_Mapping" as cksm
    #                   ON cksm.keyword_id = ckd.keyword_id
    #                    and cksm.sku = ckd.product_id
    #                   AND cksm.client_id =1
    #                   WHERE ckd.is_active
    #                   ORDER BY keyword, sku_type DESC,ckd.product_rank)t'''
    #
    #         cur.execute(query_str)
    #         messages = cur.fetchone()[0]
    #         messages = json.loads(json.dumps(messages))
    #     except Exception as e:
    #         print("Exception get input from db :-", e)
    #         messages = []
    #     return messages


if __name__ == '__main__':

    obj = DatabaseOperations()
    cur = obj.conn.cursor()
    print ("cursor ready")
    k = '[{"keyword": "chocolates", "client_id": 1, "sku": "B06ZYLPPD5", "is_competitor": false}, {"keyword": "Chocolate", "client_id": 1, "sku": "B08CSDRB6H", "is_competitor": true}, {"keyword": "Chocolate", "client_id": 1, "sku": "B097Q8F3B9", "is_competitor": false}, {"keyword": "Chocolate", "client_id": 1, "sku": "B08DRJHCSP", "is_competitor": true}, {"keyword": "Chocolate", "client_id": 1, "sku": "B075ZYRK26", "is_competitor": false}, {"keyword": "Chocolate", "client_id": 1, "sku": "B002RBTV78", "is_competitor": true}, {"keyword": "Chocolate", "client_id": 1, "sku": "B08T34YZGB", "is_competitor": false}, {"keyword": "Soccer Shoes", "client_id": 1, "sku": "B08QCX4FV1", "is_competitor": false}, {"keyword": "Soccer Shoes", "client_id": 1, "sku": "B087FCH7YD", "is_competitor": true}, {"keyword": "Soccer Shoes", "client_id": 1, "sku": "B097NSW1L5", "is_competitor": false}, {"keyword": "Soccer Shoes", "client_id": 1, "sku": "B08JMCQKHM", "is_competitor": true}, {"keyword": "Soccer Shoes", "client_id": 1, "sku": "B083TRQ37W", "is_competitor": false}, {"keyword": "Soccer Shoes", "client_id": 1, "sku": "B07RZV2KK6", "is_competitor": true}, {"keyword": "Soccer Shoes", "client_id": 1, "sku": "B08QCMTWKH", "is_competitor": false}, {"keyword": "Soccer Shoes", "client_id": 1, "sku": "B07S1Y4KQS", "is_competitor": true}, {"keyword": "Soccer Shoes", "client_id": 1, "sku": "B09NS6YT5J", "is_competitor": false}, {"keyword": "best mobile phones", "client_id": 1, "sku": "B09YT93XTS", "is_competitor": false}, {"keyword": "best mobile phones", "client_id": 1, "sku": "B07R5CJDD7", "is_competitor": true}, {"keyword": "best mobile phones", "client_id": 1, "sku": "B09NLZZH31", "is_competitor": false}, {"keyword": "best mobile phones", "client_id": 1, "sku": "B08VDXP9NP", "is_competitor": true}, {"keyword": "best mobile phones", "client_id": 1, "sku": "B08ZHNGKHB", "is_competitor": false}, {"keyword": "best mobile phones", "client_id": 1, "sku": "B09HJZPFDD", "is_competitor": true}, {"keyword": "best mobile phones", "client_id": 1, "sku": "B09B16YSL5", "is_competitor": false}, {"keyword": "best mobile phones", "client_id": 1, "sku": "B07QYBSRGC", "is_competitor": false}, {"keyword": "best mobile phones", "client_id": 1, "sku": "B0987D4B3S", "is_competitor": false}]'
    cur.callproc('"ADT_Master".update_and_fetch_client_input',[k])
    print (cur.fetchone())
    pass
