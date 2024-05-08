import os
import io
import requests
import re

import pandas as pd
import numpy as np
import webvtt
import pyktok as pyk
from moviepy.editor import VideoFileClip

from azure_connector import AzureConnector

pyk.specify_browser('chrome')

class VideoIsPrivateError(Exception):
    pass

class RequestReturnedNoneError(Exception):
    pass

class TiktokVideoDetails:
    def __init__(self, url: str):
        self.transcription_source : str
        self.url = url
        tt_json = pyk.alt_get_tiktok_json(self.url)
        if tt_json is None:
            print("ttjson is none")
            raise RequestReturnedNoneError("Json request returned None. Please try again later.")
        try:
            self.details: dict = tt_json["__DEFAULT_SCOPE__"]["webapp.video-detail"]["itemInfo"]["itemStruct"]
        except KeyError:
            print("Keyerror")
            raise VideoIsPrivateError("Video details could not be parsed. Video is private or has been removed.")

    @property
    def description(self) -> str:
        return self.details.get("desc")

    @property
    def suggested_words(self) -> list:
        return self.details.get("suggestedWords", [])

    @property
    def has_original_sound(self) -> bool:
        return self.details["music"]["authorName"] == self.details["author"]["nickname"]

    @property
    def download_url(self) -> str:
        return self.details['video']['downloadAddr']

    @property
    def duration(self) -> float:
        return float(self.details["video"]["duration"])


    def get_transcriptions(self, disable_azure: bool = False) -> dict:
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        headers = {"User-Agent": user_agent}
        transcriptions = {}

        for info in self.details["video"]["subtitleInfos"]:
            if (language := info["LanguageCodeName"]) in ["eng-US", "deu-DE"] and info["Format"] == "webvtt":
                result = requests.get(info["Url"], headers=headers)
                if vtt := result.content.decode():
                    transcript = ""
                    try:
                        for caption in webvtt.read_buffer(io.StringIO(vtt)):
                            # Some captions require an extra space in between
                            transcript += f"{caption.text} "
                    except webvtt.MalformedFileError as error:
                        print(error)
                        print(vtt)
                        continue
                    transcriptions[language] = transcript

        if transcriptions:
            self.transcription_source = "Tiktok"
        elif not disable_azure:
            if self.has_original_sound:
                transcriptions = self.get_transcription_from_azure()
                self.transcription_source = "Azure Speech to Text"

            # if not transcriptions:
            #     video_filename = re.findall(pyk.url_regex, self.url)[0].replace("/", '_') + '.mp4' # taken from pyktok
            #     transcriptions = AzureConnector.get_ocr_from_azure(self.download_url)
            #     self.transcription_source = "Azure Video Indexer"


        return transcriptions

    def save_data_to_csv_file(self, csv_filename: str):
        # Gather video meta data
        meta_data = pyk.generate_data_row(video_obj=self.details)

        transcriptions = self.get_transcriptions()

        # Add custom desired info
        meta_data["suggested_words"] = " / ".join(self.suggested_words)
        meta_data["url"] = self.url
        meta_data["transcription_source"] = self.transcription_source
        meta_data["english_transcript"] = transcriptions.get("eng-US", np.nan)
        meta_data["german_transcript"] = transcriptions.get("deu-DE", np.nan)

        if os.path.exists(csv_filename):
            df = pd.read_csv(csv_filename, index_col=0)
            meta_data = pd.concat([df, meta_data], ignore_index=True)
        else:
            print("Creating new csv file")

        meta_data.to_csv(csv_filename)

    def get_transcription_from_azure(self) -> dict:
        # save audio/video and perform speech to text
        pyk.save_tiktok(self.url)
        video_filename = re.findall(pyk.url_regex, self.url)[0].replace("/", '_') + '.mp4' # taken from pyktok
        # Get audio from tiktok
        audio = VideoFileClip(video_filename).audio
        audio_filename = 'tiktok_audio.wav'
        audio.write_audiofile(audio_filename)

        transcriptions = AzureConnector.translation_continuous_with_lid_from_multilingual_file(audio_filename)

        os.remove(video_filename)
        os.remove(audio_filename)

        return transcriptions