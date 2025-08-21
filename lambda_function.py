import os
import logging
import requests
import io
import json
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import polars as pl
import boto3
from botocore.exceptions import ClientError

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
client = WebClient(token=SLACK_BOT_TOKEN)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("MirrorCompareProcessedFiles")

try:
    auth_response = client.auth_test()
    BOT_USER_ID = auth_response["user_id"]
    logger.info(f"Initialized with Bot User ID: {BOT_USER_ID}")
except SlackApiError as e:
    logger.error(f"Failed to get bot user ID: {e}")
    BOT_USER_ID = None

def is_file_processed(file_id: str) -> bool:
    try:
        resp = table.get_item(Key={"file_id": file_id})
        return "Item" in resp
    except ClientError as e:
        logger.error(f"DynamoDB get_item failed: {e}")
        return False

def mark_file_processed(file_id: str):
    try:
        table.put_item(Item={"file_id": file_id})
    except ClientError as e:
        logger.error(f"DynamoDB put_item failed: {e}")

def run_comparison_from_string(csv_content: str, channel_id=None, thread_ts=None):
    csv_file = io.StringIO(csv_content)
    df = pl.read_csv(csv_file)
    df = df.rename({col: col.strip() for col in df.columns})

    required_columns = ["requestId", "beat.name", "res.bodyStr"]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        msg = f"CSV is missing required columns: {', '.join(missing_cols)}"
        logger.warning(msg)
        if channel_id:
            try:
                client.chat_postMessage(
                    channel=channel_id,
                    text=msg,
                    thread_ts=thread_ts
                )
            except SlackApiError as slack_err:
                logger.error(f"Failed to send missing columns message to Slack: {slack_err}")
        return ""

    def parse_body(body_str: str):
        try:
            return json.loads(body_str)
        except Exception:
            return {}

    df = df.with_columns(
        pl.col("res.bodyStr").map_elements(parse_body, return_dtype=pl.Object).alias("parsed_resbody")
    )

    prod_df = df.filter(pl.col("beat.name").str.contains("prod-dexter-name-match-eks$"))
    mirror_df = df.filter(pl.col("beat.name").str.contains("prod-dexter-name-match-mirror-eks$"))

    merged = prod_df.join(mirror_df, on="requestId", suffix="_mirror")

    report_rows = []
    for row in merged.iter_rows(named=True):
        prod = row["parsed_resbody"].get("result", {}).get("namematch", {})
        mirror = row["parsed_resbody_mirror"].get("result", {}).get("namematch", {})

        prod_status_code = row["parsed_resbody"].get("statusCode")
        mirror_status_code = row["parsed_resbody_mirror"].get("statusCode")

        report_rows.append({
            "requestId": row["requestId"],
            "value_match": prod.get("value") == mirror.get("value"),
            "score_match": prod.get("score") == mirror.get("score"),
            "statusCode_match": prod_status_code == mirror_status_code,
            "prod_value": prod.get("value"),
            "mirror_value": mirror.get("value"),
            "prod_score": prod.get("score"),
            "mirror_score": mirror.get("score"),
            "prod_statusCode": prod_status_code,
            "mirror_statusCode": mirror_status_code
        })

    report_df = pl.DataFrame(report_rows)
    output_buffer = io.StringIO()
    report_df.write_csv(output_buffer)
    return output_buffer.getvalue()

def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
    except (json.JSONDecodeError, TypeError):
        body = {}

    if body.get("type") == "url_verification":
        logger.info("Received Slack URL verification challenge.")
        return {"statusCode": 200, "body": body.get("challenge")}

    slack_event = body.get("event", {})
    event_type = slack_event.get("type")
    if event_type == "file_shared":
        event_user_id = slack_event.get("user_id")
        if event_user_id == BOT_USER_ID:
            logger.info("Ignoring event triggered by the bot itself to prevent a loop.")
            return {"statusCode": 200, "body": "Ignoring own event"}

        file_id = slack_event.get("file_id")
        channel_id = slack_event.get("channel_id")
        logger.info(f"Received file_shared event from user {event_user_id} for file: {file_id}")

        if is_file_processed(file_id):
            logger.info(f"File {file_id} already processed. Skipping.")
            return {"statusCode": 200, "body": "File already processed"}
        mark_file_processed(file_id)

        try:
            file_info_response = client.files_info(file=file_id)
            if not file_info_response["ok"]:
                raise Exception(f"Failed to get file info: {file_info_response['error']}")

            file_data = file_info_response["file"]

            parent_ts = None

            public_shares = file_data.get("shares", {}).get("public", {}).get(channel_id, [])
            if public_shares:
                parent_ts = public_shares[0]["ts"]

            if not parent_ts:
                private_shares = file_data.get("shares", {}).get("private", {}).get(channel_id, [])
                if private_shares:
                    parent_ts = private_shares[0]["ts"]

            if not parent_ts:
                parent_ts = slack_event.get("event_ts")


            if file_data.get("filetype") != "csv":
                logger.warning(f"Ignoring non-CSV file: {file_data.get('name')}")
                try:
                    client.chat_postMessage(
                        channel=channel_id,
                        text=f"Ignored file `{file_data.get('name')}` because it is not a CSV.",
                        thread_ts=parent_ts
                    )
                except SlackApiError as slack_err:
                    logger.error(f"Failed to send non-CSV message to Slack: {slack_err}")
                return {"statusCode": 200, "body": "File is not a CSV, ignoring."}

            download_url = file_data["url_private_download"]
            original_filename = file_data["name"]

            headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
            response = requests.get(download_url, headers=headers)
            response.raise_for_status()

            processed_content = run_comparison_from_string(response.text, channel_id, parent_ts)
            if not processed_content:
                return {"statusCode": 200, "body": "CSV processing skipped due to missing columns"}

            new_filename = f"report_{original_filename}"

            client.files_upload_v2(
                channel=channel_id,
                content=processed_content,
                filename=new_filename,
                initial_comment=f"Comparison report for `{original_filename}` ✅",
                thread_ts=parent_ts
            )
            logger.info(f"Successfully uploaded {new_filename} as thread reply to file {file_id}")

        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            try:
                client.chat_postMessage(
                    channel=channel_id,
                    text=f"Sorry, an error occurred while processing your file: {e}",
                    thread_ts=parent_ts if 'parent_ts' in locals() else None
                )
            except SlackApiError as slack_err:
                logger.error(f"Failed to send error message to Slack: {slack_err}")
            return {"statusCode": 500, "body": f"An unexpected error occurred: {e}"}

    return {"statusCode": 200, "body": "Event processed"}