import glob
import os
import pickle
import traceback

from apiclient import errors
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from config import conns


class GdriveConnector:
    """
    This class is created to make connection with Google Drive API for Upload files.
    """

    def __init__(self, logger):
        """
        This function is to initialization the variable to create connection with Google Drive API.
        Args:
            logger: logger object to add logs in file.
        """
        self.logger = logger
        self.path = conns["GDrive"]["Path"]
        self.gdrive_link = ""
        self.client_secret_file = conns["GDrive"]["Secrets"]
        self.api_name = conns["GDrive"]["API_Name"]
        self.api_version = conns["GDrive"]["API_VERSION"]
        self.scopes = [conns["GDrive"]["Scopes"]]
        self.folder_id = conns["GDrive"]["Folder_ID"]
        self.service = self.create_service(
            self.client_secret_file, self.api_name, self.api_version, self.scopes
        )

    def create_service(self, client_secret_file, api_name, api_version, *scopes):
        """
        This function is used to make connection with G Drive
        :param client_secret_file:
        :param api_name:
        :param api_version:
        :param scopes:
        :return:
        """
        client_secret_file = client_secret_file
        api_service_name = api_name
        api_version = api_version
        scopes = [scope for scope in scopes[0]]

        cred = None

        pickle_file = f"token_{api_service_name}_{api_version}.pickle"

        if os.path.exists(pickle_file):
            with open(pickle_file, "rb") as token:
                cred = pickle.load(token)

        if not cred or not cred.valid:
            if cred and cred.expired and cred.refresh_token:
                cred.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    client_secret_file, scopes
                )
                cred = flow.run_local_server()

            with open(pickle_file, "wb") as token:
                pickle.dump(cred, token)

        try:
            service = build(api_service_name, api_version, credentials=cred)
            self.logger.info(f"service created successfully {api_service_name}")
            return service
        except Exception as err:
            self.logger.error(f"Error! {err}")
            self.logger.error(traceback.format_exc())
            return None

    def upload_video_file(self):
        """
        This function is used to get the file path from local and upload in G drive.
        :return: G Drive download link.
        """
        try:
            # 1st list for file names, 2nd list for file types
            retrieve_all_files = self.retrieve_all_files()
            retrieve_all = [i["name"] for i in retrieve_all_files]
            file_name = self.get_latest_file()
            mime_type = "video/mp4"
            if file_name not in retrieve_all:
                file_metadata = {"name": file_name, "parents": [self.folder_id]}
                media = MediaFileUpload(
                    self.path + "/{}".format(file_name), mimetype=mime_type
                )

                res = (
                    self.service.files()
                        .create(body=file_metadata, media_body=media, fields="id")
                        .execute()
                )
                self.gdrive_link = res.get("id")
                self.logger.info(
                    "Uploaded file to {url}".format(
                        url="https://drive.google.com/open?id=" + res.get("id")
                    )
                )
            else:
                self.gdrive_link = [
                    i["id"] for i in retrieve_all_files if i["name"] == file_name
                ][0]
                self.logger.info(
                    "Already Uploaded file to {url}".format(
                        url="https://drive.google.com/open?id=" + self.gdrive_link
                    )
                )

            return "https://drive.google.com/open?id=" + self.gdrive_link
        except Exception as err:
            self.logger.error(
                f"Something went wrong on while uploading the video on GDRIVE! {err}"
            )
            self.logger.error(traceback.format_exc())

    def retrieve_all_files(self):
        """Retrieve a list of File resources.

        Returns:
          List of File resources.
        """
        result = []
        page_token = None
        while True:
            try:
                param = {}
                if page_token:
                    param["pageToken"] = page_token
                response = (
                    self.service.files()
                        .list(
                        q="'" + self.folder_id + "' in parents",
                        pageSize=1000,
                        pageToken=page_token,
                        fields="nextPageToken, files(id, name)",
                    )
                        .execute()
                )
                result.extend(response.get("files", []))
                page_token = response.get("nextPageToken")
                if not page_token:
                    break
            except errors.HttpError as error:
                self.logger.error(f"Something went wrong! {error}")
                self.logger.error(traceback.format_exc())
                break
        return result

    def delete_file(self, file_ids):
        """Permanently delete a file, skipping the trash.

        Args:
          file_ids: List of ID of the file to delete.
        """
        try:
            for file_id in file_ids:
                self.service.files().delete(fileId=file_id).execute()
        except errors.HttpError as error:
            self.logger.error(f"Something went wrong! {error}")
            self.logger.error(traceback.format_exc())

    def get_latest_file(self):
        """
        This function is get to latest downloaded file from downloaded folder
        Returns:

        """
        try:
            list_of_files = glob.glob(
                ".\\downloaded_video\\*"
            )  # * means all if need specific format then *.csv
            latest_file = max(list_of_files, key=os.path.getctime)
            return latest_file.split("""\\""")[-1]
        except Exception as error:
            self.logger.error(f"Something went wrong! {error}")
            self.logger.error(traceback.format_exc())
