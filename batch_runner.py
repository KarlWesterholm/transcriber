import logging
import json
import time

import papermill as pm

LOG = logging.getLogger("transcriber.runner")

# urls = ["https://www.tiktok.com/@afdfraktionimbundestag/video/7306473663895751969?is_from_webapp=1&sender_device=pc",
#         "https://www.tiktok.com/@wirsinddasvolkde/video/7259813032321092891?is_from_webapp=1&sender_device=pc",
#         "https://www.tiktok.com/@afd_kv_calw_freudenstadt/video/7292339785916599584?is_from_webapp=1&sender_device=pc",
#         "https://www.tiktok.com/@herrafd/video/7272913585162980640?is_from_webapp=1&sender_device=pc",
#         "https://www.tiktok.com/@clips_30494/video/7328063815403212064?is_from_webapp=1&sender_device=pc",
#         "https://www.tiktok.com/@hossundhopfpodcast108/video/7320406759863110944?is_from_webapp=1&sender_device=pc",
#         "https://www.tiktok.com/@deutschland_zuerst2/video/7293557213291875616?is_from_webapp=1&sender_device=pc"
#         "https://www.tiktok.com/@cocodrewke/video/7320464887879044385?is_from_webapp=1&sender_device=pc&web_id=7362277606560319008",
#        ]

def get_data():
    start_time = time.time()
    success_rate = {"successes": 0, "failures": 0, "failed_urls": []}

    with open("./data/afd_100000_april.json", "r") as file:
        data = json.load(file)

    try:
        for hit in data["hits"]["hits"]:
            url = f"https://www.tiktok.com/@{hit["_source"]["creator_username"]}/video/{hit["_id"]}"
            try:
                pm.execute_notebook(
                    "./data_collector.ipynb",
                    f"./papermill_output.ipynb",
                    parameters={"url": url, "filename": "afd_100000_april.csv"},
                    progress_bar=True,
                )
                LOG.info(f'Succesfully processed url: \n"{url}"'),
                success_rate["successes"] += 1
            except Exception as error:
                LOG.error(f'Encountered following problem with url: \n"{url}"')
                LOG.error(error)
                success_rate["failures"] += 1
                success_rate["failed_urls"].append(url)

    except KeyboardInterrupt:
        print("Keyboard interrupt detected. Stopping...")
    finally:
        end_time = time.time()

        print("Successes:", success_rate["successes"])
        print("Failures:", success_rate["failures"])
        print("Failed URLs:")
        for link in success_rate["failed_urls"]:
            print(link)
        LOG.info(success_rate)
        print("Total elapsed time:", (end_time - start_time))

if __name__ == '__main__':
    logging.basicConfig(
    filename="executions.log",
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    datefmt="%d-%m-%Y %H:%M:%S"
    )
    get_data()
