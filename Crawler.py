import datetime
import os.path
import time
from random import  randint
from bs4 import BeautifulSoup
from requests import request
from selenium import webdriver
import json
from tqdm import tqdm



class Crawler :

    user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 Edg/92.0.902.55",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11.5; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_5_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/534.34 (KHTML, like Gecko) Qt/4.8.5 Safari/534.34",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
  ]
    output_json = {"Keyword":{}
                   }

    def __init__(self,sqs_obj,db_obj):

        self.launch_browser()
        self.input_queue_name = "CrawlerInput"
        self.output_queue_name = "CrawlerOutput"
        self.db_obj = db_obj
        print ("**Crawler Engine Init\n**************************************\n")

        self.start_crawling(sqs_obj)



    def start_crawling(self,sqs_obj):
        output = []
        messages = sqs_obj.get_message_from_queue()

        for message in tqdm(range(len(messages))):
            message = messages[message]

            try:
                meta = json.loads(message.body)
                keyword_id = meta.get('keyword_id')
                response = self.crawl(meta).copy() or []
                print("**Scraping Results\n")
                time.sleep(2)
                for obj in response:
                    final_res_obj = {}
                    final_res_obj['keyword_id'] = keyword_id
                    final_res_obj['is_active'] = True
                    final_res_obj['sku'] = obj.get('Product ID','')
                    final_res_obj['product_name'] =obj.get('Name','')
                    final_res_obj['product_price'] = obj.get('Price')
                    final_res_obj['product_rank'] =obj.get('Rank')
                    final_res_obj['product_image_url'] = obj.get('Image Url')
                    final_res_obj['organic'] = obj.get('Is Sponsored')
                    final_res_obj['product_url'] = obj.get('Url')
                    final_res_obj['product_review_count'] = obj.get('Reviews')
                    final_res_obj['product_rating'] = obj.get('Rating')
                    output.append(final_res_obj.copy())
                    # print("**Message Results Scraped.")
            except Exception as e:
                print (message)
                print ("Exception in Parse response start crawling -: ",e)
            message.delete()
        time.sleep(2)
        # try:
        #     with open('Results/Output_JSON.json','w') as d:
        #         json.dump(output,d)
        # except Exception as e:
        #     print ("Exception json dump ",e)
        #     pass


        self.db_obj.insert_crawled_records(json.dumps(output))
        print("**Crawled Records Inserted In DB.")

        return


    def crawl(self,meta):
        headers = self.get_headers()
        current_page = meta.get("current_page")
        keyword = meta.get("keyword")
        keyword_id = meta.get('keyword_id')
        keyword_tag = keyword.replace(" ","+")
        url = "https://www.amazon.ca/s?k={}".format(keyword_tag)
        try:
            retryCount = 1
            maxRetry = 5
            while retryCount <=maxRetry:
                now = datetime.datetime.now()
                date_time = now.strftime("%m-%d-%Y %H_%M_%S")
                if not os.path.exists("Crawled Pages/{}".format(keyword)):
                    os.mkdir("Crawled Pages/{}".format(keyword))
                file_name = "Crawled Pages/{}/{}_Page_{}_.html".format(keyword,date_time,current_page)
                response = request("GET",url=url,headers=headers)
                if response.status_code != 200:
                    print ("Status Code: ",response.status_code)
                    retryCount +=1
                    continue
                if retryCount ==maxRetry:
                    return {}
                response = response.content
                self.project_html(response,file_name)
                return self.extract_keyword_data(response,meta)
        except Exception as e:
            print ("Exception in Crawl Method :",e)
            return []
    #
    def project_html(self,response,file_name):
        try:
            if not self.driver:
                return
            with open(file_name,"wb") as d:
                d.write(response)
            file_name = "file:////Users/deepanshusharma/Documents/University/ADT/ADT_Project/"+file_name
            self.driver.get(file_name)

        except Exception as e:
            pass
    #
    def extract_keyword_data(self,response,meta):
        products_response = []
        rank = meta.get("rank", 0)
        try:
            soup = BeautifulSoup(response, 'html.parser')
            attrs_list = [{"data-component-type":"s-search-result"},
                          ]
            sequence_of_products = []
            for attr in attrs_list:
                sequence_of_products = soup.find_all('div',attrs=attr) or []
                if len(sequence_of_products) > 4:
                    break
            for product in sequence_of_products:
                product_info_response = {}
                rank +=1
                product_rank = rank
                try:
                    product_id = product["data-asin"]
                except Exception as e:
                    product_id = None
                try:
                    image = product.find('img',attrs={"class":"s-image"})['src']
                except Exception as e:
                    image = None

                if product_id:
                    product_url = "https://www.amazon.ca/product_name/dp/"+product_id
                else:
                    product_url = None

                try:
                    product_name = product.find('span',attrs={"class":"a-size-base-plus a-color-base a-text-normal"})
                    if not product_name:
                        product_name = product.find('span',
                                            attrs={"class": "a-size-medium a-color-base a-text-normal"})
                    product_name = product_name.text
                except Exception as e:
                    product_name = None
                try:
                    product_rating = product.find('div',attrs={"class": "a-section a-spacing-none a-spacing-top-micro"}).find("div",attrs={"class": "a-row a-size-small"}).find("span",attrs={"class":"a-icon-alt"}).text
                    product_rating = product_rating.replace("out of 5 stars", "")
                    product_rating = product_rating.strip()
                    product_rating = float(product_rating)
                except Exception as e:
                    product_rating = None

                try:
                    number_of_views = product.find('div',attrs={"class": "a-section a-spacing-none a-spacing-top-micro"}).find("div",attrs={"class": "a-row a-size-small"}).find("span",attrs={"class":"a-size-base s-underline-text"}).text
                    number_of_views = int(number_of_views)
                except Exception as e:
                    number_of_views = None

                try:
                    price = product.find('span',attrs = {"class": "a-price"})
                    price = price.find('span',attrs={"class":"a-offscreen"}).text
                    price = price.replace("$","")
                    price = price.strip()
                    price = float(price)
                except Exception as e:
                    price = None
                try:
                    sponsored = product.find("a",attrs={"aria-label": "View Sponsored information or leave ad feedback"})
                    sponsored = sponsored.text
                    if "Sponsored" in sponsored:
                        sponsored = True
                    else:
                        sponsored = False
                except Exception as e:
                    sponsored = False

                product_info_response['Rank'] = product_rank
                product_info_response['Product ID'] = product_id
                product_info_response['Name'] = product_name
                product_info_response['Url'] = product_url
                product_info_response['Price'] = price
                product_info_response['Image Url'] = image
                product_info_response['Rating'] = product_rating
                product_info_response['Reviews'] = number_of_views
                product_info_response['Is Sponsored'] = sponsored

                products_response.append(product_info_response)
        except Exception as e:
            print ("Exception occurred Extract Data: ",e)

        return products_response

    def get_user_agent(self):
        n = len(self.user_agents)
        n = randint(0,n)
        return self.user_agents[n]

    def get_headers(self):
        headers = {}
        user_agent = self.get_user_agent()
        headers['User-Agent'] = user_agent
        headers['Host'] = "www.amazon.ca"
        return headers
    #
    def launch_browser(self):
        try:
            driver = webdriver.Chrome(executable_path="/Users/deepanshusharma/PycharmProjects/WDET_PROJECT/chromedriver 3")
            driver.set_window_size(1400, 900)
            driver.get("file:////Users/deepanshusharma/PycharmProjects/WDET_PROJECT/Crawled Pages/Static/main.html")
            print("Chrome Browser Initialised Successfully")
            time.sleep(4)
            driver.get("file:////Users/deepanshusharma/PycharmProjects/WDET_PROJECT/Crawled Pages/Static/Initialised.html")
            time.sleep(3)
        except Exception as ee:
            print("Chrome driver not loaded, Trying to load from the absolute path now:-",ee)
            driver = None
        self.driver = driver
