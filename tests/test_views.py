from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.views.views import Views


class TestViews:
    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "views", "config")

    def test_views(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestViews",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                views = Views(configuration, retriever, tempdir)

                models = views.get_models_list()
                assert models == [
                    {
                        "Dataset": "fatalities002_2025_01_t01",
                        "Model name": "fatalities",
                        "Model version": "002",
                        "Last input data": "2025-01",
                        "Forecasting window": "2025-02 - 2028-01",
                        "Release date": "2025-02-24",
                        "Codebook": "https://api.viewsforecasting.org/"
                        "fatalities002_2025_01_t01/codebook",
                    },
                    {
                        "Dataset": "fatalities002_2024_12_t01",
                        "Model name": "fatalities",
                        "Model version": "002",
                        "Last input data": "2024-12",
                        "Forecasting window": "2025-01 - 2027-12",
                        "Release date": "2025-01-27",
                        "Codebook": "https://api.viewsforecasting.org/"
                        "fatalities002_2024_12_t01/codebook",
                    },
                    {
                        "Dataset": "fatalities002_2024_11_t01",
                        "Model name": "fatalities",
                        "Model version": "002",
                        "Last input data": "2024-11",
                        "Forecasting window": "2024-12 - 2027-11",
                        "Release date": "2025-01-11",
                        "Codebook": "https://api.viewsforecasting.org/"
                        "fatalities002_2024_11_t01/codebook",
                    },
                ]

                latest_cm_data = views.get_api_data(models[0]["Dataset"], "cm")

                locations = views.get_locations(latest_cm_data["data"])
                assert locations == [
                    {"code": "AFG", "name": "Afghanistan"},
                    {"code": "ALB", "name": "Albania"},
                    {"code": "DZA", "name": "Algeria"},
                    {"code": "XKX", "name": "Kosovo"},
                ]

                datasets = views.generate_datasets()
                dataset = datasets[0]
                dataset.update_from_yaml(path=join(config_dir, "hdx_dataset_static.yaml"))

                assert dataset == {
                    "name": "afg-views-conflict-forecasts",
                    "title": "Afghanistan - VIEWS conflict forecasts",
                    "dataset_date": "[2025-02-01T00:00:00 TO 2028-01-01T23:59:59]",
                    "tags": [
                        {
                            "name": "conflict-violence",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "fatalities",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "forecasting",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "license_id": "cc-by-sa",
                    "methodology": "https://viewsforecasting.org/early-warning-system/definitions/",
                    "caveats": "None",
                    "dataset_source": "Violence & Impacts Early-Warning System",
                    "groups": [{"name": "afg"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "b682f6f7-cd7e-4bd4-8aa7-f74138dc6313",
                    "owner_org": "03d86d43-6add-4e30-a510-72a475e57fa3",
                    "data_update_frequency": 30,
                    "notes": "The Violence & Impacts Early-Warning System (VIEWS) is an "
                    "award-winning conflict prediction system that generates monthly "
                    "forecasts for violent conflicts across the world up to three years "
                    "in advance. It is supported by the iterative research and "
                    "development activities undertaken by the VIEWS consortium.",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "afg-views-conflict-forecasts-country-month.csv",
                        "description": "CSV of monthly predictions for impending state-based "
                        "conflict up to three years in advance. The forecasts "
                        "are presented as point predictions for the number of fatalities per "
                        "country and month. See the [codebook]"
                        "(https://api.viewsforecasting.org/fatalities002_2025_01_t01/codebook) "
                        "for a description of available variables.",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "CSV of monthly predictions for impending state-based "
                        "conflict up to three years in advance. The "
                        "forecasts are presented as point predictions for the number "
                        "of fatalities per prio-grid cell and month. See the [codebook]"
                        "(https://api.viewsforecasting.org/fatalities002_2025_01_t01/codebook) "
                        "for a description of available variables.",
                        "format": "csv",
                        "name": "afg-views-conflict-forecasts-priogrid-month.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
