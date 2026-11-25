# Traffic Mirror Compare Bot

An AWS Lambda function that works as a Slack bot. When someone uploads a CSV of logged API responses to a Slack channel, the bot compares the production responses with the mirrored-traffic responses and replies in the thread with a comparison report.

## How it works

1. Slack sends a `file_shared` event to the Lambda function (the function also answers Slack's `url_verification` challenge). Events caused by the bot's own uploads are ignored.
2. The function checks a DynamoDB table (`MirrorCompareProcessedFiles`) so the same file is never processed twice.
3. Non-CSV files are ignored and the bot says so in the thread.
4. The CSV is downloaded from Slack and read with Polars. It must contain the columns `requestId`, `beat.name` and `res.bodyStr`. If any are missing, the bot replies with which ones.
5. Rows are split by `beat.name` into the production service (`prod-dexter-name-match-eks`) and its mirror (`prod-dexter-name-match-mirror-eks`), and joined on `requestId`.
6. For every request, the function parses the JSON response body and compares the `namematch` result `value`, the `score` and the `statusCode` of both versions.
7. The bot uploads `report_<original file name>` to the thread. It has one row per request with `value_match`, `score_match`, `statusCode_match` and the production and mirror values.

## Configuration

| Item | Value |
|---|---|
| Environment variable | `SLACK_BOT_TOKEN` |
| DynamoDB table | `MirrorCompareProcessedFiles` (partition key `file_id`) |
| Python packages | `slack_sdk`, `polars`, `boto3`, `requests` |

Deploy `lambda_function.py` as a Lambda function behind an HTTP endpoint (for example API Gateway or a function URL) and set it as the Slack app's event request URL. The Slack app needs access to file events and permission to read files, upload files and post messages.
