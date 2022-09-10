import base64
import json
import os
import re
import shutil
import time
import traceback

import pandas as pd
import requests
from bs4 import BeautifulSoup as Bs
from pytube import YouTube
from selenium import webdriver

from config import conns
from gdrive_upload import GdriveConnector
from mongodb_connection import MongoDBConnect


class YoutubeScrapper:
    """
    This class is used to get information about videos.
    """

    def __init__(self, executable_path, chrome_options, logger):
        """
        This function initializes the web browser driver
        :param executable_path: chrome driver path
        :param chrome_options:
        """
        try:
            self.logger = logger
            self.urls = []
            self.title_list = []
            self.view = []
            self.upload_time = []
            self.channel_name_list = []
            self.comments = []
            self.commenter_name = []
            self.likes = []
            self.no_of_comments = []
            self.video_title = []
            self.video_url = []
            self.thumbnail = []
            self.gdrive_url = None
            self.gdrive_url_list = []
            self.im_b64 = None
            self.get_im_b64 = None
            self.user_id = []
            self.download_path = ".\\downloaded_video"
            self.driver = webdriver.Chrome(
                executable_path=executable_path, chrome_options=chrome_options
            )
        except Exception as err:
            self.logger.error(
                f"Something went wrong on initializing webdriver object: {err}"
            )
            self.logger.error(traceback.format_exc())

    def open_url(self, url):
        """
        This function open the particular url passed.
        param url: URL to be opened.
        """
        try:
            if self.driver:
                self.driver.get(f"{url}")
                return True
            else:
                return False
        except Exception as err:
            self.logger.error(f"Error! {err}")
            self.logger.error(traceback.format_exc())

    def search_video(self, search_id, record_count, channel_df):
        """
        This function helps to search video and get the video details.
        """
        try:
            channel_df.drop_duplicates(
                subset=["VIDEO_TITLE", "VIDEO_LINK"], keep=False, inplace=True
            )
            titles_list = channel_df["VIDEO_TITLE"].unique()
            content = self.driver.page_source.encode("utf-8").strip()
            soup = Bs(content, "html.parser")
            titles = soup.findAll("a", id="video-title")
            views = soup.findAll("span", class_="style-scope ytd-grid-video-renderer")
            channel_name = soup.findAll(
                "yt-formatted-string", {"class": "style-scope ytd-channel-name"}
            )[0].text
            video_urls = soup.findAll("a", id="video-title")
            i = 0  # views and time
            j = 0  # urls
            time.sleep(2)

            self.delete_video()
            self.logger.info("Video deleted in local folder")
            gdrive_object = GdriveConnector(self.logger)
            self.logger.info("Created G Drive connector")
            for idx, title in enumerate(titles[:record_count]):

                if title not in titles_list:
                    my_video = YouTube(
                        conns["Links"]["YouTube"] + video_urls[j].get("href")
                    )
                    self.channel_name_list.append(channel_name)
                    self.title_list.append(title.text)
                    self.view.append(views[idx].text)
                    self.upload_time.append(views[idx + 1].text)
                    self.urls.append(video_urls[idx].get("href"))
                    self.thumbnail.append(my_video.thumbnail_url)
                    self.user_id.append(search_id)
                    i += 2
                    # j += 1
                    mp4files = my_video.streams.filter(file_extension="mp4")
                    time.sleep(2)

                    try:
                        # downloading the video
                        mp4files.first().download("downloaded_video/")
                        self.logger.info(
                            f"Downloading video {my_video.title.encode('utf-8').decode('ascii', 'ignore')}"
                        )

                        # Upload in G Drive
                        time.sleep(2)
                        self.gdrive_url = gdrive_object.upload_video_file()

                        # Get G drive video link
                        self.gdrive_url_list.append(self.gdrive_url)
                    except Exception as err:
                        self.logger.error(f"Error! {err}")
                        self.logger.error(traceback.format_exc())
                else:
                    i += 2
                    continue
                self.logger.info(f"Video Uploaded in G Drive: {self.gdrive_url}")

            time.sleep(4)

            data = {
                "USERID": self.user_id,
                "CHANNEL_NAME": self.channel_name_list,
                "VIDEO_TITLE": self.title_list,
                "VIDEO_LINK": self.urls,
                "VIEWS": self.view,
                "UPLOAD_TIME": self.upload_time,
                "THUMBNAIL": self.thumbnail,
                "VIDEO_DOWNLOAD_LINK": self.gdrive_url_list,
            }
            df = pd.DataFrame(data)
            time.sleep(4)

            df.drop_duplicates(
                subset=["VIDEO_TITLE", "VIDEO_LINK"], keep=False, inplace=True
            )

            return df
        except Exception as err:
            self.logger.error(f"Error! {err}")
            self.logger.error(traceback.format_exc())

    def video_info(self, search_id):
        """
        This function is used to get video info from url
        :return:
        """
        try:
            video_url_link = conns["Links"]["YouTube"]
            mongo_client = MongoDBConnect(
                username=conns["MongoDB"]["UserName"],
                password=conns["MongoDB"]["Password"],
                logger=self.logger,
            )
            self.logger.info(f"Urls: {self.urls}")
            self.user_id = []
            for no, i in enumerate(self.urls[: len(self.urls)]):
                if "/watch" in i:
                    url = video_url_link + i
                    self.open_url(url)
                    prev_h = 0
                    while True:
                        height = self.driver.execute_script(
                            """
                                function getActualHeight() {
                                    return Math.max(
                                        Math.max(document.body.scrollHeight, document.documentElement.scrollHeight),
                                        Math.max(document.body.offsetHeight, document.documentElement.offsetHeight),
                                        Math.max(document.body.clientHeight, document.documentElement.clientHeight)
                                    );
                                }
                                return getActualHeight();
                            """
                        )
                        self.driver.execute_script(
                            f"window.scrollTo({prev_h},{prev_h + 200})"
                        )
                        # fix the time sleep value according to your network connection
                        time.sleep(1)
                        prev_h += 200
                        if prev_h >= height:
                            break
                    soup = Bs(self.driver.page_source, "html.parser")
                    title_text_div = soup.select_one("#container h1")
                    video_title = title_text_div and title_text_div.text
                    comment_div = soup.select("#content #content-text")
                    author_div = soup.select("#content #author-text")
                    count = soup.select_one("#comments")

                    data = re.search(
                        r"var ytInitialData = ({.*?});", soup.prettify()
                    ).group(1)
                    data_json = json.loads(data)
                    videoPrimaryInfoRenderer = data_json["contents"][
                        "twoColumnWatchNextResults"
                    ]["results"]["results"]["contents"][0]["videoPrimaryInfoRenderer"]
                    videoSecondaryInfoRenderer = data_json["contents"][
                        "twoColumnWatchNextResults"
                    ]["results"]["results"]["contents"][1]["videoSecondaryInfoRenderer"]
                    # number of likes
                    likes_label = videoPrimaryInfoRenderer["videoActions"][
                        "menuRenderer"
                    ]["topLevelButtons"][0]["toggleButtonRenderer"]["defaultText"][
                        "accessibility"
                    ][
                        "accessibilityData"
                    ][
                        "label"
                    ]  # "No likes" or "###,### likes"
                    likes_str = likes_label.split(" ")[0].replace(",", "")
                    likes_count = "0" if likes_str == "No" else likes_str

                    if len(comment_div) == 0:
                        self.commenter_name.append("")
                        self.comments.append("")
                        self.no_of_comments.append(str(len(comment_div)))
                        self.video_title.append(video_title)
                        self.video_url.append(i)
                        self.likes.append(likes_count)
                        self.user_id.append(search_id)

                    else:
                        self.comments = []
                        for idx, val in enumerate(comment_div):
                            self.commenter_name.append(
                                author_div[idx].span.text.strip()
                            )
                            self.comments.append(val.text)
                            self.no_of_comments.append(count.span.text)
                            self.video_title.append(video_title)
                            self.video_url.append(i)
                            self.likes.append(likes_count)
                            self.user_id.append(search_id)

                    self.get_im_b64 = self.image_to_base64(self.thumbnail[no])
                    comments_dict = {
                        "VIDEO_TITLE": video_title,
                        "COMMENTS": self.comments,
                        "THUMBNAIL_IMB64": self.get_im_b64,
                    }
                    mongo_client.insert_records(
                        db_name=conns["MongoDB"]["Database"],
                        collection_name=conns["MongoDB"]["CollectionName"],
                        records=comments_dict,
                    )
                    self.logger.info("Data inserted in mongodb")

            self.driver.quit()

            video_info = {
                "USERID": self.user_id,
                "VIDEO_LINK": self.video_url,
                "VIDEO_TITLE": self.video_title,
                "NO_OF_COMMENTS": self.no_of_comments,
                "COMMENTER_NAME": self.commenter_name,
                "LIKES": self.likes,
            }
            df = pd.DataFrame(video_info)
            return df
        except Exception as err:
            # self.driver.refresh()
            self.logger.error(f"Error! {err}")
            self.logger.error(traceback.format_exc())

    def delete_video(self):
        """
        This functon isi used to delete the content inside the folder
        :return:
        """
        folder = self.download_path
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as err:
                self.logger.error(f"Failed to delete {file_path}. Reason: %s! {err} ")
                self.logger.error(traceback.format_exc())

    def image_to_base64(self, thumbnail_url):
        """
        This function is used for image processing tasks.
        :param thumbnail_url: List of thumbnail image urls
        :return: get the list of images in binary format
        """

        self.im_b64 = base64.b64encode(requests.get(thumbnail_url, timeout=5).content)
        self.logger.info("Created thumbnail urls image to base64 object")

        return self.im_b64
