#!/usr/bin/python
"""views scraper"""

import logging
from datetime import datetime
from typing import List

import requests
from bs4 import BeautifulSoup
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Views:
    def __init__(self, configuration: Configuration, retriever: Retrieve, temp_dir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir

    def get_locations(self, data) -> List:
        countries = {row["name"] for row in data}
        locations = []
        for country in list(countries):
            iso3 = Country.get_iso3_country_code_fuzzy(country)

            if iso3[0] is None:  # kosovo
                print("could not match", country)
                continue

            location_name = Country.get_country_name_from_iso3(iso3[0])
            locations.append({"code": iso3[0], "name": location_name})

        locations.sort(key=lambda x: x["name"])
        return locations

    def get_models_list(self) -> List:
        """Parse wiki page to get table contents
        Args:
            None

        Returns:
            List of available datasets
        """
        response = requests.get(self._configuration["wiki_url"])
        html_content = response.text

        # Parse the HTML
        soup = BeautifulSoup(html_content, "html.parser")
        table = soup.find("table", {"role": "table"})
        rows = table.find_all("tr")

        # Get headers
        headers = [th.text.strip() for th in rows[0].find_all("th")]

        # Extract data as a list of dictionaries
        models_list = []
        for row in rows[1:]:  # Skip headers
            cells = row.find_all("td")
            row_data = {}

            for i in range(len(cells)):
                if cells[i].find("a"):
                    row_data[headers[i]] = cells[i].find("a")["href"]
                else:
                    row_data[headers[i]] = cells[i].text.strip()

            models_list.append(row_data)

        return models_list

    def get_api_data(self, run, loa, filters="") -> dict:
        base_url = self._configuration["base_url"]
        api_url = f"{base_url}{run}/{loa}{filters}"
        print(api_url)
        try:
            data = self._retriever.download_json(api_url)
            return data
        except DownloadError as e:
            logger.error(f"Could not get data from {api_url} {e}")
            return {}

    def get_date(self, month_id: int) -> str:
        """Convert a VIEWS month_id to a human-readable month and year."""
        start_year = 1980
        start_month = 1

        # Calculate year and month directly
        total_months = month_id - 1  # Because month_id=1 is January 1980
        year = start_year + (total_months // 12)
        month = start_month + (total_months % 12)

        return f"{datetime(year, month, 1).strftime('%B %d %Y')}"  # Format: 'Month Year'

    def generate_datasets(self) -> List:
        models = self.get_models_list()
        codebook_url = models[0]["Codebook"]
        latest_run = models[0]["Dataset"]
        latest_cm_data = self.get_api_data(latest_run, "cm")  # get data from latest dataset
        locations = self.get_locations(latest_cm_data["data"])
        start_date = self.get_date(latest_cm_data["start_date"])
        end_date = self.get_date(latest_cm_data["end_date"])

        datasets = []
        for location in locations[:1]:
            # Create dataset
            dataset_info = self._configuration
            dataset_title = f"{location['name']} - {dataset_info['title']}"
            slugified_name = slugify(dataset_title)

            dataset = Dataset({"name": slugified_name, "title": dataset_title})

            # Add dataset info
            dataset.add_country_location(location["code"])
            dataset.add_tags(dataset_info["tags"])
            dataset.set_time_period(start_date, end_date)

            # Create country-month resource
            cm_data = self.get_api_data(latest_run, "cm", f"?iso={location['code']}")

            resource_name = f"{slugified_name}-country-month.csv"
            resource_description = dataset_info["description"]
            resource = {
                "name": resource_name,
                "description": resource_description,
            }
            dataset.generate_resource_from_rows(
                self._temp_dir,
                resource_name,
                cm_data["data"],
                resource,
                list(cm_data["data"][0].keys()),
                encoding="utf-8",
            )

            # Create PRIO-GRID-month resource
            pgm_data = self.get_api_data(latest_run, "pgm", f"?iso={location['code']}")
            if pgm_data["data"]:
                pgm_resource_name = f"{slugified_name}-priogrid-month.csv"
                pgm_resource_description = dataset_info["description"]
                pgm_resource = {
                    "name": pgm_resource_name,
                    "description": pgm_resource_description,
                }
                dataset.generate_resource_from_rows(
                    self._temp_dir,
                    pgm_resource_name,
                    pgm_data["data"],
                    pgm_resource,
                    list(pgm_data["data"][0].keys()),
                    encoding="utf-8",
                )

            datasets.append(dataset)

        # Create codebook dataset
        dataset_info = self._configuration
        dataset_title = f"Codebook - {dataset_info['title']}"
        slugified_name = slugify(dataset_title)

        dataset = Dataset({"name": slugified_name, "title": dataset_title})

        # Add dataset info
        dataset.add_other_location("world")
        dataset.add_tags(dataset_info["tags"])
        dataset.set_time_period(start_date, end_date)

        # Create resource
        resource = {
            "name": f"{slugified_name}.json",
            "description": "Codebook containing information on model variables",
            "url": codebook_url,
            "format": "JSON",
        }
        dataset.add_update_resource(resource)
        datasets.append(dataset)

        return datasets
