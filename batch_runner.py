import papermill as pm

urls = ["https://www.tiktok.com/@afdfraktionimbundestag/video/7306473663895751969?is_from_webapp=1&sender_device=pc",
        "https://www.tiktok.com/@wirsinddasvolkde/video/7259813032321092891?is_from_webapp=1&sender_device=pc",
        "https://www.tiktok.com/@afd_kv_calw_freudenstadt/video/7292339785916599584?is_from_webapp=1&sender_device=pc",
        "https://www.tiktok.com/@herrafd/video/7272913585162980640?is_from_webapp=1&sender_device=pc",
        "https://www.tiktok.com/@clips_30494/video/7328063815403212064?is_from_webapp=1&sender_device=pc",
        "https://www.tiktok.com/@hossundhopfpodcast108/video/7320406759863110944?is_from_webapp=1&sender_device=pc",
        "https://www.tiktok.com/@deutschland_zuerst2/video/7293557213291875616?is_from_webapp=1&sender_device=pc"
        "https://www.tiktok.com/@cocodrewke/video/7320464887879044385?is_from_webapp=1&sender_device=pc&web_id=7362277606560319008",
       ]

for url in urls:
    pm.execute_notebook(
        "./data_collector.ipynb",
        "./papermill_output.ipynb",
        parameters={"url": url},
        progress_bar=True,
    )