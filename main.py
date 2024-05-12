import os
import time

import pandas as pd
import numpy as np

from tiktok_video_details import (
    TiktokVideoDetails,
    VideoIsPrivateError,
    RequestReturnedNoneError,
    HTTPRequestError
)

class StatCollector:
    def __init__(self):
        self.start_time = time.time()
        self.successes = 0
        self.private_videos = []
        self.failed_requests = []

    def add_success(self):
        self.successes += 1

    def add_private_video(self, url: str):
        self.private_videos.append(url)

    def add_failed_request(self, url: str):
        self.failed_requests.append(url)

    def print_stats(self):
        end_time = time.time()
        print("\n")
        print("Private: \n", "\n\t".join(self.private_videos))
        print("Failed: \n", "\n\t".join(self.failed_requests))
        print("Successes: ", self.successes)
        print("Private: ", len(self.private_videos))
        print("Failed: ", len(self.failed_requests))
        total_time = end_time - self.start_time
        hours = total_time // 3600
        minutes = (total_time % 3600) // 60
        seconds = total_time - 3600 * hours - 60 * minutes
        print("Total elapsed time: %dh %dm %.2fs" % (
            hours,
            minutes,
            seconds
        ))

def print_progress_bar(percentage: float, bar_length: int = 20) -> None:
    normalizer = int(100 / bar_length)
    progress = '\r[%s%s] %.2f%%' % (
        '='*int(percentage/normalizer),
        ' '*int(bar_length - percentage/normalizer),
        percentage
    )
    print(progress, end='', flush=True)

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

            # tt_obj.save_data_to_csv_file("transcriptions.csv")
            successes += tt_obj.save_data_to_csv_file("transcriptions.csv")
    except KeyboardInterrupt:
        print("Keyboard Interrupt detected. Stopping...")
    finally:
        print("Private: \n", "\n".join(private_videos))
        print("Failed: \n", "\n".join(failed_requests))
        print("Successes: ", successes)
        print("Private: ", len(private_videos))
        print("Failed: ", len(failed_requests))

def save_tiktok_info_to_existing_csv(csv_filename: str):

    df = pd.read_csv(csv_filename)

    total_rows = len(df)
    errors = {}
    en_transcriptions = {}
    de_transcriptions = {}

    stats = StatCollector()

    try:
        for index, row in df.iterrows():
            completion_percentage = (index/total_rows)*100
            print_progress_bar(completion_percentage)
            # url = f"https://www.tiktok.com/@{row["author_username"]}/video/{row["video_id"]}"
            url = row["url"]
            try:
                tt_obj = TiktokVideoDetails(url=url)
            except VideoIsPrivateError as error:
                stats.add_private_video(url)
                print('\n', error)
                errors[index] = error
                continue
            except (RequestReturnedNoneError, HTTPRequestError) as error:
                stats.add_failed_request(url)
                print('\n', error)
                errors[index] = error
                continue
            except Exception as error:
                stats.add_failed_request(url)
                print('\nUnexpected Exception occured:', error)
                errors[index] = error
                continue


            try:
                transcriptions = tt_obj.get_transcriptions(disable_azure=True)
                if transcriptions:
                    stats.add_success()
                else:
                    errors[index] = "No transcription provided by Tiktok"
            except Exception as error:
                print('\n', error)
                transcriptions = {}
                errors[index] = error

            en_transcriptions[index] = transcriptions.get("eng-US", np.nan)
            de_transcriptions[index] = transcriptions.get("deu-DE", np.nan)
    except KeyboardInterrupt:
        print("\nKeyboard Interrupt detected. Stopping...")
    except Exception as error:
        print("\nUnexpected Exception occurred:", error)
    finally:
        stats.print_stats()

        target_filename = os.path.splitext(csv_filename)[0] + '_transcribed.csv'
        target_filename = 'test2.csv'
        new_df = df.assign(
            english_transcript=en_transcriptions,
            german_transcript=de_transcriptions,
            error_reason=errors)
        new_df.to_csv(target_filename)

if __name__ == '__main__':
    save_tiktok_info_to_existing_csv(csv_filename="./data/tiktok_videos_based_on_hashtags_cleaned.csv")