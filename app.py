import traceback
import warnings

import pandas as pd
from flask import Flask, redirect, render_template, request, url_for
from flask_cors import CORS, cross_origin
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

from config import conns
from logger_class import getLog
from mongodb_connection import MongoDBConnect
from snowflakes_connection import SnowflakesConn
from youtube_scrapping import YoutubeScrapper

warnings.simplefilter(action="ignore", category=FutureWarning)

logger = getLog("youtube")

app = Flask(__name__)  # initialising the flask app with the name 'app'

# For selenium driver implementation on heroku
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("disable-dev-shm-usage")


@app.route("/", methods=["GET"])  # route to display the home page
@cross_origin()
def home_page():
    """
    This function is render to index page.
    Returns:

    """
    return render_template("index.html")


@app.route("/video", methods=["GET", "POST"])
@cross_origin()
def index():
    """
    This function is used to display video info on UI and if data is not available in database then fetch the data
    from YouTube video and store in database.
    Returns:

    """
    if request.method == "POST":
        expected_video = int(request.form["expected_video"])
        search_string = request.form["content"].replace(
            " ", ""
        )  # obtaining the search string entered in the form
        try:
            search_id = search_string.split("/")[-1]
            conn = SnowflakesConn(logger)
            logger.info("Connected with snowflakes")
            channel_df = conn.select_data("CHANNEL_VIDEOS", search_id)
            video_count = channel_df["USERID"].count()
            if video_count > expected_video:
                logger.info("Data is available in database")
                return redirect(
                    url_for("results", messages=search_id, expected_val=expected_video)
                )

            else:
                return redirect(url_for("nodata"))

        except Exception as err:
            logger.error(f"Error {err}")
            logger.error(traceback.format_exc())
    else:
        return render_template("index.html")


@app.route("/new_request", methods=["GET", "POST"])
@cross_origin()
def new_request():
    """
    This function is used to display video info on UI and if data is not available in database then fetch the data
    from YouTube video and store in database.
    Returns:

    """
    if request.method == "POST":
        expected_video = int(request.form["expected_video"])
        search_string = request.form["content"].replace(
            " ", ""
        )  # obtaining the search string entered in the form
        try:
            search_id = search_string.split("/")[-1]
            conn = SnowflakesConn(logger)
            logger.info("Connected with snowflakes")
            youtube_object = YoutubeScrapper(
                executable_path=ChromeDriverManager().install(),
                chrome_options=chrome_options,
                logger=logger,
            )
            no_rec = expected_video
            youtube_object.open_url(search_string + "/videos")
            logger.info("Open the URL")
            channel_df = conn.select_data("CHANNEL_VIDEOS", search_id)
            search_object = youtube_object.search_video(search_id, no_rec, channel_df)
            logger.info("Stored channel name and video details in database")
            conn = SnowflakesConn(logger)
            conn.insert_data(search_object, "CHANNEL_VIDEOS")
            logger.info("Stored each video details in database")
            res = youtube_object.video_info(search_id)
            if len(res) != 0:
                conn.insert_data(res, "VIDEO_INFO")
                logger.info("Stored each video comments in Mongodb")

                return redirect(
                    url_for(".results", messages=search_id, expected_val=expected_video)
                )
            else:
                return "Not enough videos"
        except Exception as err:
            logger.error(f"Error! {err}")
            logger.error(traceback.format_exc())


@app.route("/results")
@cross_origin()
def results():
    """
    This function is used to get all video info and simply on ui
    Returns:

    """
    global comment_df
    search_id = request.args["messages"]  # counterpart for url_for()
    fetch_count = request.args["expected_val"]  # counterpart for url_for()

    logger.info(f"search_id: {search_id} ")

    try:
        conn = SnowflakesConn(logger)
        mongo_client = MongoDBConnect(
            username=conns["MongoDB"]["UserName"], password=conns["MongoDB"]["Password"], logger=logger
        )
        channel_data_df = conn.select_no_data("CHANNEL_VIDEOS", search_id, fetch_count)
        video_data_df = conn.select_data("VIDEO_INFO", search_id)
        df1 = pd.merge(
            channel_data_df,
            video_data_df,
            how="inner",
            on=["VIDEO_LINK", "VIDEO_TITLE"],
        )
        titles_list = df1["VIDEO_TITLE"].unique()
        df2 = pd.DataFrame(columns=["VIDEO_TITLE", "COMMENTS"])

        for i in titles_list:
            comment_row = mongo_client.find_records(
                db_name=conns["MongoDB"]["Database"],
                collection_name=conns["MongoDB"]["CollectionName"],
                title=i,
            )
            for j in comment_row:
                comment_df = pd.DataFrame(j)
            df2 = df2.append(comment_df, ignore_index=True)
        df2.drop("VIDEO_TITLE", axis=1, inplace=True)
        df3 = pd.concat([df1, df2], axis=1)
        final_data = df3.to_dict("records")
        return render_template("results.html", data=final_data)
    except Exception as err:
        logger.error(f"Error! {err}")
        logger.error(traceback.format_exc())


@app.route("/nodata")  # route to display the home page
@cross_origin()
def nodata():
    """
    This function is render to error page.
    Returns:

    """

    return render_template("error.html")


@app.route("/feedback")  # route to display the home page
@cross_origin()
def feedback():
    """
    This function is render to error page.
    Returns:

    """

    return render_template("new_request.html")


if __name__ == "__main__":
    app.run()
