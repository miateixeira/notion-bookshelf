import argparse
import json
import requests
from urllib.parse import urljoin
import math
from datetime import datetime
import pandas as pd

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

###################
#  Notion Client  #
###################

class NotionClient():
    def __init__(self, notion_key):
        """
        Set up a session for Notion requests
        """
        self.notion_key = notion_key
        self.default_headers = {
            'Authorization': f"Bearer {self.notion_key}",
            'Content-Type': 'application/json',
            'Notion-Version': '2022-06-28'
        }
        self.session = requests.Session()
        self.session.headers.update(self.default_headers)
        self.NOTION_BASE_URL = "https://api.notion.com/v1/"

    def query_database(self, db_id, filter_object=None, sorts=None, start_cursor=None, page_size=None):
        """
        Make paginated requests against a Notion database
        """
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
        """
        Make a request for a particular property on a particular Notion page
        This is needed to get the title of a page in a Relation
        """
        page_url = urljoin(self.NOTION_BASE_URL, f"pages/{page_id}/properties/{property_id}")
        return self.session.get(page_url)

    def update_page(self, page_id, data):
        """
        Updates an existing Notion page
        """
        page_url = urljoin(self.NOTION_BASE_URL, f"pages/{page_id}")
        return self.session.patch(page_url, data=data)

    def create_page(self, db_id, properties, icon_url):
        """
        Makes a new page in the indicated Notion database
        """
        page_url = urljoin(self.NOTION_BASE_URL, f"pages/")
        payload = {
            "parent": { "database_id": db_id },
            "icon": {
                "type": "external",
                "external": { "url": icon_url }
            },
            "properties": properties
        }
        data = json.dumps(payload)
        return self.session.post(page_url, data=data)

##################
# Filter creator #
##################

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

    def create_filter(self):
        """
        Create a filter object based on the passed args
        """
        filters = []
        # Only query for items that have not been transferred to the new database
        not_transferred = { "property": "Transferred to new db?", "checkbox": { "equals": False } }
        filters.append(not_transferred)

        # Create filter for items matching the status provided in the args
        if self.status is not None:
            status = { "property": "Status", "multi_select": { "contains": self.status } }
            filters.append(status)

        # Create filter for items of the requested type (e.g. Book, Novella, Graphic Novel, etc.)
        if self.args.type is not None:
            media_type = { "property": "Type", "select": { "equals": self.args.type } }
            filters.append(mediat_type)

        filter_object = self.dict_list_to_object(filters)
        return filter_object

    def dict_list_to_object(self, filters):
        """
        Convert a list of dicts to single filter object
        """
        if len(filters) > 1:
            # This recursion makes the resulting nested dict as wide/shallow as possible
            # I'm not sure why, but having too many levels of nesting causing an error
            # when querying the database
            f1 = self.dict_list_to_object(filters[:len(filters)//2])
            f2 = self.dict_list_to_object(filters[len(filters)//2:])
            return { "and": [f1, f2] }
        return filters[0]

####################
# Pandas converter #
####################

class PandasConverter():
    def __init__(self):
        self.supported_properties = [
            "checkbox", "date", "number", "rich_text",
            "title", "files", "select", "multi_select", "relation"
        ]
        self.text_types = ["rich_text", "title"]

    def response_to_records(self, db_response):
        """
        Convert a list of JSON response item into a dict of records
        """
        records = []
        for result in db_response["results"]:
            records.append(self.get_record(result))
        return records

    def get_record(self, result):
        """
        Convert a single JSON response item to a record
        """
        record = {}
        record["page_id"] = result['id']
        for name in result["properties"]:
            if self.is_supported(result["properties"][name]):
                record[name] = self.get_property_value(result["properties"][name])
        return record

    def is_supported(self, prop):
        """
        Return True if the property is currently supported, False otherwise
        """
        if prop.get("type") in self.supported_properties:
            return True
        else:
            return False

    def get_property_value(self, prop):
        """
        Call the appropriate method for processing a property based on its type
        """
        prop_type = prop.get("type")

        if prop_type in self.text_types:
            # rich_text and title types can be treated the same
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
            # returns numbers and checkboxes as is
            return prop.get(prop_type)

    def get_text(self, text_object):
        """
        Return the plain text of a rich_text or title type property
        """
        text = ""
        text_type = text_object.get("type")
        for rt in text_object.get(text_type):
            text += rt.get("plain_text")
        return text

    def get_date(self, date_object):
        """
        Convert ISO date from Notion into a datetime object
        """
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
        """
        Get the url to the external file object
        """
        files_object = file_object.get("files")
        if not files_object: # list of files not empty
            return None
        else:
            if files_object[0].get("type") == "external":
                external_file_object = files_object[0].get("external")
                return external_file_object.get("url")
            return None

    def get_select(self, select_object):
        """
        Get the name of the Notion select object's value
        """
        select_value = select_object.get("select")
        if select_value is not None:
            return select_value.get("name")
        return None

    def get_multi_select(self, multi_select_object):
        """
        Get a list of the names of the Notion multi-select object's values
        """
        multi_select_value = multi_select_object.get("multi_select")
        values = []
        for selection in multi_select_value:
            values.append(selection.get("name"))
        return values

    def get_relation(self, relation_object):
        """
        Get a list of the titles of the pages in the Notion relation object
        """
        relation_value = relation_object.get("relation")
        if not relation_value:
            return None
        else:
            titles = []
            for rel in relation_value:
                rel_id = rel.get("id")
                titles.append(self.get_relation_title(rel_id))
            return titles

    def get_relation_title(self, page_id):
        """
        Query a Notion page and return its title as plain text
        """
        tmp_client = NotionClient(SECRET_KEY) # temporary Notion client
        page_response = tmp_client.query_page_property(page_id, "title")
        if page_response.ok:
            page_response_obj = page_response.json()
            results_obj = page_response_obj.get("results")[0]
            title_obj = results_obj.get("title")
            return title_obj.get("plain_text")
        return None

#################
# Pandas loader #
#################

class PandasLoader():
    def __init__(self, filter_creator, notion_client, pandas_converter):
        self.filter_creator = filter_creator
        self.notion_client = notion_client
        self.converter = pandas_converter

        self.filter_object = self.filter_creator.create_filter()

    def load_db(self, db_id):
        """
        Query a Notion database and load its contents into a Pandas DataFrame
        """
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
    def __init__(self, notion_client, df, args):
        self.notion_client = notion_client
        self.df = df
        self.args = args

        # TODO: Add the other types to this dict
        self.properties_needed_by_type = {
            'Book': [
                'Title', '0 Type', '0 Cover', '1 Alternate title', '1 Author(s)',
                '1 Language', '1 Genre(s)', '1 Dates read', '1 Rating',
                'BNG Current page', 'BNG Total pages', 'BNG Number in series',
                'BNGISP Publication date', 'BNGCA Owned'
            ]
        }

    def transfer_entries(self):
        if self.args.text:
            self.df = self.df.sample(n=1)

        for index, row in self.df.iterrows():
            cover_url = self.get_cover_url(row)
            properties = self.compile_properties(row, cover_url)
            
            response = self.notion_client.create_page(NEW_DATABASE_ID, properties, cover_url)
            if not response.ok:
                print(response.text)
            else:
                print("Transfer successful!")

            self.update_transferred(row["page_id"])

    def get_cover_url(self, entry):
        continue

    def compile_properties(self, entry, cover_url):
        continue

    def update_transferred(self, page_id):
        # This should maybe be a method of the Notion client instead?
        continue

#############################
#  Putting it all together  #
#############################

def main(args):
    client = NotionClient(SECRET_KEY)
    filter_creator = FilterCreator(args)
    converter = PandasConverter()
    loader = PandasLoader(filter_creator, client, converter)
    df = loader.load_db(OLD_DATABASE_ID)

    deployer = PayloadDeployer(client, df, args)
    deployer.transfer_entries()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-test', '--test', action='store_true', help="only process first matching entry")
    parser.add_argument('-s', '--status', type=int, help="0 for Read, 1 for Reading, 2 for Want to read")
    parser.add_argument('-t', '--type', type=str, help="e.g. Book, Novella, etc.")
    args = parser.parse_args()
    main(args)
