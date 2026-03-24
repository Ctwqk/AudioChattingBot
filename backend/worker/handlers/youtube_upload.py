import os
import shutil
from pathlib import Path
from worker.handlers.base import BaseHandler


class YouTubeUploadHandler(BaseHandler):
    """Upload a video to YouTube using the YouTube Data API v3.

    Requires a valid OAuth2 credentials file at the path specified by
    the YOUTUBE_CREDENTIALS_DIR environment variable (default: ~/.youtube_credentials).
    """

    async def execute(self, node_config, input_paths, output_path):
        input_file = input_paths["input"]
        title = node_config.get("title", "Untitled")
        description = node_config.get("description", "")
        privacy = node_config.get("privacy", "private")
        tags_str = node_config.get("tags", "")
        tags = [t.strip() for t in tags_str.split(",") if t.strip()] if tags_str else []

        # Try to upload via YouTube API
        cred_dir = os.environ.get("YOUTUBE_CREDENTIALS_DIR", os.path.expanduser("~/.youtube_credentials"))
        client_secret = None
        for f in Path(cred_dir).glob("client_secret*.json"):
            client_secret = str(f)
            break

        if not client_secret:
            raise RuntimeError(
                f"YouTube credentials not found in {cred_dir}. "
                "Mount a directory containing client_secret*.json and token.json."
            )

        await self._upload_youtube(
            input_file, title, description, privacy, tags, cred_dir, client_secret
        )

        # Copy input to output_path for artifact tracking
        shutil.copy2(input_file, output_path)

    async def _upload_youtube(
        self, video_path, title, description, privacy, tags, cred_dir, client_secret
    ):
        """Upload using google-api-python-client."""
        try:
            from google.oauth2.credentials import Credentials
            from googleapiclient.discovery import build
            from googleapiclient.http import MediaFileUpload
        except ImportError:
            raise RuntimeError(
                "YouTube upload dependencies are missing in the worker image. "
                "Install google-api-python-client/google-auth-oauthlib for the worker."
            )

        token_path = os.path.join(cred_dir, "token.json")
        if not os.path.exists(token_path):
            raise RuntimeError(
                f"Missing OAuth token at {token_path}. "
                "Generate it via the YouTubeManager auth flow before using youtube_upload."
            )

        creds = Credentials.from_authorized_user_file(
            token_path,
            scopes=["https://www.googleapis.com/auth/youtube.upload"],
        )

        if not creds.valid:
            if creds.expired and creds.refresh_token:
                from google.auth.transport.requests import Request
                creds.refresh(Request())
                with open(token_path, "w") as f:
                    f.write(creds.to_json())
            else:
                raise RuntimeError(
                    f"OAuth token at {token_path} is invalid and cannot be refreshed. "
                    "Re-authorize via YouTubeManager."
                )

        youtube = build("youtube", "v3", credentials=creds)

        body = {
            "snippet": {
                "title": title,
                "description": description,
                "tags": tags,
                "categoryId": "22",  # People & Blogs
            },
            "status": {
                "privacyStatus": privacy,
            },
        }

        media = MediaFileUpload(video_path, mimetype="video/*", resumable=True)
        request = youtube.videos().insert(
            part="snippet,status",
            body=body,
            media_body=media,
        )

        response = None
        while response is None:
            _, response = request.next_chunk()

        import logging
        logging.getLogger("worker").info(
            f"YouTube upload complete: video_id={response.get('id')}"
        )
