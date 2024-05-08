import argparse

import pandas as pd

from tiktok_video_details import TiktokVideoDetails, VideoIsPrivateError, RequestReturnedNoneError

def get_tiktok_info(url: str = None, csv_filename: str = None):
    if url is None and csv_filename is None:
        raise ValueError("No url or csv file specified.")

    if url:
        urls = [url]
    elif csv_filename:
        df = pd.read_csv(csv_filename)
        urls = [f"https://www.tiktok.com/@{row["author_username"]}/video/{row["video_id"]}" for _, row in df.iterrows()]

    successes = 0
    private_videos = []
    failed_requests = []

    try:
        for url in urls:
            try:
                tt_obj = TiktokVideoDetails(url=url)
            except VideoIsPrivateError as error:
                private_videos.append(url)
                print(error)
                continue
            except RequestReturnedNoneError as error:
                failed_requests.append(url)
                print(error)
                continue

            tt_obj.save_data_to_csv_file("transcriptions.csv")
            successes += 1
    except KeyboardInterrupt:
        print("Keyboard Interrupt detected. Stopping...")
    finally:
        print("Private: \n", "\n".join(private_videos))
        print("Failed: \n", "\n".join(failed_requests))
        print("Successes: ", successes)
        print("Private: ", len(private_videos))
        print("Failed: ", len(failed_requests))



if __name__ == '__main__':
    get_tiktok_info(csv_filename="./data/all_data_01-01-2024_06-05-2024.csv")