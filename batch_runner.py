import logging
import json

import papermill as pm

logging.basicConfig(
    filename="executions.log",
    filemode="a",
    )

# urls = ["https://www.tiktok.com/@afdfraktionimbundestag/video/7306473663895751969?is_from_webapp=1&sender_device=pc",
#         "https://www.tiktok.com/@wirsinddasvolkde/video/7259813032321092891?is_from_webapp=1&sender_device=pc",
#         "https://www.tiktok.com/@afd_kv_calw_freudenstadt/video/7292339785916599584?is_from_webapp=1&sender_device=pc",
#         "https://www.tiktok.com/@herrafd/video/7272913585162980640?is_from_webapp=1&sender_device=pc",
#         "https://www.tiktok.com/@clips_30494/video/7328063815403212064?is_from_webapp=1&sender_device=pc",
#         "https://www.tiktok.com/@hossundhopfpodcast108/video/7320406759863110944?is_from_webapp=1&sender_device=pc",
#         "https://www.tiktok.com/@deutschland_zuerst2/video/7293557213291875616?is_from_webapp=1&sender_device=pc"
#         "https://www.tiktok.com/@cocodrewke/video/7320464887879044385?is_from_webapp=1&sender_device=pc&web_id=7362277606560319008",
#        ]

success_rate = {"successes": 0, "failures": 0, "failed_urls": []}

with open("./data/afd_100000_april.json", "r") as file:
    data = json.load(file)

for hit in data["hits"]["hits"]:
    url = f"https://www.tiktok.com/@{hit["_source"]["creator_username"]}/video/{hit["_id"]}"
    try:
        pm.execute_notebook(
            "./data_collector.ipynb",
            f"./papermill_output.ipynb",
            parameters={"url": url, "filename": "afd_100000_april.csv"},
            progress_bar=True,
        )
        logging.info(f'Succesfully processed url: \n"{url}"'),
        success_rate["successes"] += 1
    except Exception as error:
        logging.error(f'Encountered following problem with url: \n"{url}"')
        logging.error(error)
        success_rate["failures"] += 1
        success_rate["failed_urls"].append(url)

print(success_rate)
logging.info(success_rate)
