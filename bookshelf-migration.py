import argparse
import pandas as pd
import requests
from requests.exceptions import HTTPError
import json
import math
import pprint
from urllib.parse import urljoin
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait

################
#  Parameters  #
################

def get_keys(path):
    with open(path) as f:
        return json.load(f)

keys = get_keys('~/.secret/keys.json')

SECRET_KEY = keys['NOTION_SECRET_KEY']
NEW_DATABASE_ID = keys['NOTION_NEW_DATABASE_ID']
OLD_DATABASE_ID = keys['NOTION_OLD_DATABASE_ID']
GOOGLE_API_KEY = keys['GOOGLE_API_KEY']

###################
#  Notion Client  #
###################

class NotionClient():
    # Set up a session for Notion requests
    def __init__(self, notion_key):
        self.notion_key = notion_key
        self.default_headers = {
            'Authorization': f"Bearer {self.notion_key}",
            'Content-Type': 'application/json',
            'Notion-Version': '2022-06-28'
        }
        self.session = requests.Session()
        self.session.headers.update(self.default_headers)
        self.NOTION_BASE_URL = "https://api.notion.com/v1/"

    # Allows us to make paginated requests against a Notion database
    def query_database(self, db_id, filter_object=None, sorts=None, start_cursor=None, page_size=None):
        db_url = urljoin(self.NOTION_BASE_URL, f"databases/{db_id}/query")
        params = {}
        if filter_object is not None:
            params["filter"] = filter_object
        if sorts is not None:
            params["sorts"] = sorts
        if start_cursor is not None:
            params["start_cursor"] = start_cursor
        if page_size is not None:
            params["page_size"] = page_size

        return self.session.post(db_url, json=params)

    def query_page_property(self, page_id, property_id):
        page_url = urljoin(self.NOTION_BASE_URL, f"pages/{page_id}/properties/{property_id}")
        return self.session.get(page_url)

    def update_page(self, page_id, data):
        page_url = urljoin(self.NOTION_BASE_URL, f"pages/{page_id}")
        return self.session.patch(page_url, data=data)

    def create_page(self, db_id, properties, icon_url):
        """
        Creates a page in the NEW database
        """
        page_url = urljoin(self.NOTION_BASE_URL, f"pages/")
        payload = {
            "parent": { "database_id": db_id },
            "icon": {
                "type": "external",
                "external": {
                    "url": icon_url
                }
            },
            "properties": properties
        }
        data = json.dumps(payload)
        return self.session.post(page_url, data=data)

#########################################################
#  Convert response to something Pandas can understand  #
#########################################################

class PandasConverter():
    text_types = ["rich_text", "title"]

    # Create records out of an entire list of results
    def response_to_records(self, db_response):
        records = []
        for result in db_response["results"]:
            records.append(self.get_record(result))
        return records

    # Parse a record out of an individual Notion item
    def get_record(self, result):
        record = {}
        record["page_id"] = result['id']
        for name in result["properties"]:
            if self.is_supported(result["properties"][name]):
                record[name] = self.get_property_value(result["properties"][name])
        return record

    # We can use this to filter out properties we aren't currently supporting
    def is_supported(self, prop):
        if prop.get("type") in ["checkbox", "date", "number", "rich_text", "title", "files", "select", "multi_select", "relation"]:
            return True
        else:
            return False

    def get_property_value(self, prop):
        prop_type = prop.get("type")
        if prop_type in self.text_types:
            return self.get_text(prop)
        elif prop_type == "date":
            return self.get_date(prop)
        elif prop_type == "files":
            return self.get_file(prop)
        elif prop_type == "select":
            return self.get_select(prop)
        elif prop_type == "multi_select":
            return self.get_multi_select(prop)
        elif prop_type == "relation":
            return self.get_relation(prop)
        else:
            return prop.get(prop_type) # returns numbers and checkboxes as is

    def get_text(self, text_object):
        text = ""
        text_type = text_object.get("type")
        for rt in text_object.get(text_type):
            text += rt.get("plain_text")
        return text

    def get_date(self, date_object):
        date_value = date_object.get("date")
        dates = []
        if date_value is not None:
            if date_value.get("end") is None:
                dates.append(date_value.get("start"))
            else:
                start = datetime.fromisoformat(date_value.get("start"))
                end = datetime.fromisoformat(date_value.get("end"))
                dates.extend([date_value.get("start"), date_value.get("end")])
        return dates

    def get_file(self, file_object):
        files_object = file_object.get("files")
        if not files_object: # list of files not empty
            return None
        else:
            if files_object[0].get("type") == "external":
                external_file_object = files_object[0].get("external")
                return external_file_object.get("url")
            return None

    def get_select(self, select_object):
        select_value = select_object.get("select")
        if select_value is not None:
            return select_value.get("name")
        return None

    def get_multi_select(self, multi_select_object):
        multi_select_value = multi_select_object.get("multi_select")
        values = []
        for selection in multi_select_value:
            values.append(selection.get("name"))
        return values

    def get_relation(self, relation_object):
        relation_value = relation_object.get("relation")
        if not relation_value:
            return None
        else:
            titles = []
            for rel in relation_value:
                rel_id = rel.get("id")
                titles.append(self.relation_helper(rel_id))
            return titles

    def relation_helper(self, page_id):
        tmp_client = NotionClient(SECRET_KEY)
        page_response = tmp_client.query_page_property(page_id, "title")
        if page_response.ok:
            page_response_obj = page_response.json()
            results_obj = page_response_obj.get("results")[0]
            title_obj = results_obj.get("title")
            return title_obj.get("plain_text")
        return None


###################
#  Pandas Loader  #
###################

class PandasLoader():
    def __init__(self, filter_creator, notion_client, pandas_converter):
        self.notion_client = notion_client
        self.converter = pandas_converter
        self.filter_creator = filter_creator
        self.filter_object = self.filter_creator.create_filter()

    def load_db(self, db_id):
        page_count = 1
        print(f"Loading page {page_count}")
        db_response = self.notion_client.query_database(db_id, filter_object=self.filter_object)
        records = []
        if db_response.ok:
            db_response_obj = db_response.json()
            records.extend(self.converter.response_to_records(db_response_obj))

            while db_response_obj.get("has_more"):
                page_count += 1
                print(f"Loading page {page_count}")
                start_cursor = db_response_obj.get("next_cursor")
                db_response = self.notion_client.query_database(db_id, start_cursor=start_cursor)
                if db_response.ok:
                    db_response_obj = db_response.json()
                    records.extend(self.converter.response_to_records(db_response_obj))

        return pd.DataFrame(records)

######################
#  Payload deployer  #
######################

class PayloadDeployer():

    def __init__(self, notion_client, google_client, ol_client, df, args):
        self.notion_client = notion_client
        self.google_client = google_client
        self.ol_client = ol_client

        self.df = df
        self.args = args

        self.properties_needed_by_type = {
            'Book': [
                'Title', '0 Type', '0 Cover', '1 Alternate title', '1 Author(s)',
                '1 Language', '1 Genre(s)', '1 Dates read', '1 Rating',
                'BNG Current page', 'BNG Total pages', 'BNG Number in series',
                'BNGISP Publication date', 'BNGCA Owned'
            ]
        }

    def get_cover_url(self, entry):
        if entry["Cover"] is not None:
            return entry["Cover"]

        isbn = self.google_client.get_isbn(title=entry["Name"], author=entry["Author"])
        if isbn is not None:
            cover_url = self.ol_client.get_cover_url(isbn)

        return cover_url

    def get_cover_property(self, cover_url):
        if cover_url is None:
            return None

        cover = {
            'type' : 'files',
            'files': [{
                'type': 'external',
                'name': 'cover',
                'external': {
                    'url': cover_url
                }
            }]
        }
        return cover

    def get_title_property(self, entry):
        name = entry["Name"]

        if name is None:
            return None

        title = {
            'type': 'title',
            'id': 'title',
            'title': [{
                'text': {
                    'content': name
                }
            }]
        }
        return title

    def get_type_property(self, entry):
        need_no_change = ['Book'] # types that have changed names
        old_type = entry["Type"]
        new_type = None

        if old_type in need_no_change:
            new_type = old_type

        if new_type is None:
            return None

        type_property = {
            'type': 'select',
            'select': {
                'name': new_type
            }
        }

        return type_property

    def get_author_property(self, entry):
        author = entry["Author"]
        author_list = []

        for a in author:
            author_list.append({ 'name': a })

        if not author_list:
            return None

        authors_property = {
            'type': 'multi_select',
            'multi_select': author_list
        }

        return authors_property

    def get_language_property(self, entry):
        if entry["Language of original publication"] is not None:
            lang = entry["Language of original publication"]
        else:
            lang = None
            #TODO use books API to get info
            # lang = self.books_client.get_language()

        if lang is None:
            return None

        lang_property = {
            'type': 'select',
            'select': { 'name': lang }
        }
        
        return lang_property

    def get_genre_property(self, entry):
        genres = []
        if entry["Genre"] is not None:
            for g in entry["Genre"]:
                genres.append({ 'name': g })
        # else:
            # TODO books API get genres

        if not genres:
            return None

        genres_property = {
            'type': 'multi_select',
            'multi_select': genres
        }

        return genres_property

    def get_dates_property(self, entry):
        start_and_end = entry["Start and End"]

        if not start_and_end:
            return None
        elif len(start_and_end) == 1:
            start = start_and_end[0]
            end = None
        elif len(start_and_end) == 2:
            start = start_and_end[0]
            end = start_and_end[1]

        dates_property = {
            'type': 'date',
            'date': {
                'start': start,
                'end': end
            }
        }
        return dates_property

    def get_rating_property(self, entry):
        if entry["Rate"] is None:
            return None

        rating_property = {
            'type': 'select',
            'select': { 'name': entry["Rate"] }
        }
        return rating_property

    def get_current_page_property(self, entry):
        if entry["Type"] not in ['Book', 'Novella', 'Graphic Novel']:
            return None
        if math.isnan(entry['Currently on']):
            return None
        current_page = {
            'type': 'number',
            'number': entry['Currently on']
        }
        return current_page

    def get_total_pages_property(self, entry):
        if entry["Type"] not in ['Book', 'Novella', 'Graphic Novel']:
            return None
        if math.isnan(entry['Total Pages']):
            return None
        total_pages = {
            'type': 'number',
            'number': entry['Total Pages']
        }
        return total_pages

    def get_number_in_series_property(self, entry):
        if entry["Type"] not in ['Book', 'Novella', 'Graphic Novel']:
            return None
        if entry['Number in Series'] is None:
            return None
        if math.isnan(entry['Number in Series']):
            return None
        number_in_series = {
            'type': 'number',
            'number': entry['Number in Series']
        }
        return number_in_series

    def get_publication_date_property(self, entry):
        # TODO use books API to get pub date
        return None

    def get_owned_property(self, entry):
        owned_property = {
            'type': 'checkbox',
            'checkbox': entry["Owned"]
        }
        return owned_property

    def retrieve_property_value(self, prop, entry, cover_url):
        if prop == "0 Cover":
            return self.get_cover_property(cover_url)
        elif prop == "Title":
            return self.get_title_property(entry)
        elif prop == "0 Type":
            return self.get_type_property(entry)
        elif prop == "1 Author(s)":
            return self.get_author_property(entry)
        elif prop == "1 Language":
            return self.get_language_property(entry)
        elif prop == "1 Genre(s)":
            return self.get_genre_property(entry)
        elif prop == "1 Dates read":
            return self.get_dates_property(entry)
        elif prop == "1 Rating":
            return self.get_rating_property(entry)
        elif prop == "BNG Current page":
            return self.get_current_page_property(entry)
        elif prop == "BNG Total pages":
            return self.get_total_pages_property(entry)
        elif prop == "BNG Number in series":
            return self.get_number_in_series_property(entry)
        elif prop == "BNGISP Publication date":
            return self.get_publication_date_property(entry)
        elif prop == "BNGCA Owned":
            return self.get_owned_property(entry)
        else:
            print(f'Unsupported property name requested: {prop}')
            return None
        

    def compile_properties(self, entry, cover_url):
        properties = {}
        properties['Needs Review'] = { 'checkbox': True }

        for prop in self.properties_needed_by_type[entry["Type"]]:
            prop_value = self.retrieve_property_value(prop, entry, cover_url)
            if prop_value is not None:
                properties[prop] = prop_value

        return properties

    def update_transferred(self, page_id):
        payload = {
            "properties": {
                'Transferred to new db?': {
                    'checkbox': True
                }
            }
        }
        data = json.dumps(payload)
        update_response = self.notion_client.update_page(page_id, data)
        return update_response

    def create_payload(self, entry):
        cover_url = self.get_cover_url(entry)
        properties = self.compile_properties(entry, cover_url)
        response = self.notion_client.create_page(NEW_DATABASE_ID, properties, cover_url)
        if not response.ok:
            print(response.text)
        else:
            print("Transfer successful!")
        return response

    def transfer_entries(self):
        if self.args.test:
            df = self.df.sample(n=1)
        else:
            df = self.df

        for index, row in df.iterrows():
            self.create_payload(row)
            # self.update_transferred(row["page_id"])
            # TODO uncomment this when ready to actually do the transfer

##########################
#  Filter Object Creator #
##########################

class FilterCreator():
    def __init__(self, args):
        self.args = args

        self.status = None
        if self.args.status == 0:
            self.status = "Read"
        elif self.args.status == 1:
            self.status = "Reading"
        elif self.args.status == 2:
            self.status = "Want to read"

        self.year_page_id = {
            "2023": "fb9c8939252942189c50cec14df3e7bd",
            "2022": "2ee1e919789e4594b1e9f5b9c2ddf05e",
            "Grad": "d94f0c714723481994d9c3bfefdfb681",
            "Childhood": "e640e16b6a7f4a7788264a689067bc7d"
        }

    def create_filter(self):
        filters = []
        not_transferred = { "property": "Transferred to new db?", "checkbox": { "equals": False } }
        filters.append(not_transferred)

        if self.status is not None:
            status = { "property": "Status", "multi_select": { "contains": self.status } }
            filters.append(status)

        if self.args.type is not None:
            media_type = { "property": "Type", "select": { "equals": self.args.type } }
            filters.append(media_type)

        if self.args.standalone:
            standalone = { "property": "Series", "relation": { "is_empty": True } }
            filters.append(standalone)

        if self.args.year is not None:
            year = { "property": "Year Read", "relation": { "contains": self.year_page_id[self.args.year] } }
            filters.append(year)

        filter_object = self.dict_list_to_object(filters)
        return filter_object

    def dict_list_to_object(self, filters):
        if len(filters) > 1:
            return { "and": [self.dict_list_to_object(filters[:len(filters)//2]), self.dict_list_to_object(filters[len(filters)//2:])] }
        return filters[0]

#########################
#  Google Books Client  #
#########################

class GoogleBooksClient():
    def __init__(self, API_KEY, selenium_client):
        self.api_key = API_KEY
        self.selenium_client = selenium_client
        self.default_headers = {
            'Content-Type': 'application/json; charset=utf-8'
        }
        self.session = requests.Session()
        self.session.headers.update(self.default_headers)
        self.GOOGLE_BOOKS_BASE_URL = "https://www.googleapis.com/books/v1/volumes?q="
        self.GOOGLE_BOOKS_VOLUME_BASE_URL = "https://google.com/books/edition/_/"

    def query(self, title=None, author=None):
        search_terms = []

        if title is not None:
            search_terms.append("intitle:" + "+".join(title.split()))
        if (author is not None) and author:
            if type(author) == list:
                search_terms.append(f"inauthor:" + "+".join(author[0].split()))
            else:
                search_terms.append(f"inauthor:" + "+".join(author.split()))

        search_term = '&'.join(search_terms)
        search_url = self.GOOGLE_BOOKS_BASE_URL + search_term
        try:
            response = self.session.get(search_url)
            response.raise_for_status()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')
        else:
            response.encoding = 'utf-8'
            return response.json()

    def get_google_books_url(self, title=None, author=None):
        response = self.query(title, author)
        book_id = response["items"][0]["id"]
        url = self.GOOGLE_BOOKS_VOLUME_BASE_URL + book_id
        return url

    def get_original_language(self, title=None, author=None):
        url = self.get_google_books_url(title, author)
        lang = self.selenium_client.get_original_language(url)
        return lang

    def get_genres(self, title=None, author=None):
        url = self.get_google_books_url(title, author)
        genres = self.selenium_client.get_genres(url)
        return genres

    def get_isbn(self, title=None, author=None):
        response = self.query(title, author)
        identifiers = response["items"][0]["volumeInfo"]["industryIdentifiers"]#["imageLinks"]["thumbnail"] + ".png"
        for x in identifiers:
            if x["type"] == "ISBN_13":
                isbn13 = x["identifier"]
            if x["type"] == "ISBN_10":
                isbn10 = x["identifier"]

        if isbn10 not in vars():
            return isbn13
        elif isbn13 not in vars():
            return isbn10
        else:
            print(f"NO ISBN FOUND FOR {title}")

########################
#  OpenLibrary Client  #
########################

class OpenLibraryClient():
    def __init__(self):
        self.default_headers = {
            'Content-Type': 'application/json'
        }
        self.session = requests.Session()
        self.session.headers.update(self.default_headers)
        self.OL_BASE_URL = "https://openlibrary.org/api/books?bibkeys=ISBN:"

    def get_cover_url(self, isbn):
        search_url = self.OL_BASE_URL + isbn + "&format=json"
        try:
            response = self.session.get(search_url)
            response.raise_for_status()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')
        else:
            cover_url_small = response.json()[f'ISBN:{isbn}']["thumbnail_url"]
            cover_url = cover_url_small.replace('-S.jpg', '-M.jpg')
            return cover_url

#####################
#  Selenium Client  #
#####################

class SeleniumClient():
    banned_genres = ["Fiction", "fiction", "Novel", "Literary"]

    def __init__(self):
        self.driver_path = "./chromedriver"
        self.brave_path = "/usr/bin/brave"
        self.options = webdriver.ChromeOptions()
        self.options.binary_location = self.brave_path
        self.service = Service(self.driver_path)
        self.driver = webdriver.Chrome(service=self.service, options=self.options)

    def get_original_language(self, url):
        self.driver.get(url)
        lang = self.driver.find_element(By.XPATH, "//*[contains(text(), 'Original language')]/following::a")
        return lang.text

    def get_genres(self, url):
        self.driver.get(url)
        genres = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Genres')]/following-sibling::span/span/child::*")
        genres = [g.text for g in genres]
        subject = self.driver.find_element(By.XPATH, "//*[text()='Subject']/following-sibling::span/span")
        subject_clean = subject.text.replace(', more', '').replace(', Fiction', '').split(' / ')
        genres += subject_clean
        genres = [g for g in genres if not any([s in g for s in self.banned_genres])]
        return genres

#############################
#  Putting it all together  #
#############################

def main(args):
    client = NotionClient(SECRET_KEY)

    if args.create_df:
        filter_creator = FilterCreator(args)
        converter = PandasConverter()
        loader = PandasLoader(filter_creator, client, converter)
        df = loader.load_db(OLD_DATABASE_ID)
        df.to_pickle("old_database.pkl")

    df = pd.read_pickle("old_database.pkl")
    print(df.shape)
    print(df.keys())
    df = df[df["Cover"].isnull()] # TODO: for testing purposes, need to remove

    selenium_client = SeleniumClient()
    google_client = GoogleBooksClient(GOOGLE_API_KEY, selenium_client)
    ol_client = OpenLibraryClient()
    deployer = PayloadDeployer(client, google_client, ol_client, df, args)
    deployer.transfer_entries()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(epilog="WARNING: the Notion API query will not work with more than three of these filter flags in use")
    parser.add_argument('--create_df', action='store_true', help='if true, re-queries the old database and creates a new pickle file with the df')
    parser.add_argument('-test', '--test', action='store_true', help="only process first matching entry")
    parser.add_argument('-s', '--status', type=int, help="0 for Read, 1 for Reading, 2 for Want to read")
    parser.add_argument('-t', '--type', type=str, help="e.g. Book, Novella, etc.")
    parser.add_argument('-sa', '--standalone', action='store_true', help="only select standalone entries, i.e. Series property is empty")
    parser.add_argument('-y', '--year', type=str, help="year read (2023, 2022, High School -- Grad School, Childhood")
    args = parser.parse_args()
    main(args)
