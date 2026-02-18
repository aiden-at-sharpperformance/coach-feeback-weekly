import argparse
import logging
import logging.handlers
import os
import sys
from datetime import datetime, timedelta, timezone

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import snowflake.connector
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from jinja2 import Environment, FileSystemLoader

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

_file_handler = logging.handlers.RotatingFileHandler(
    "status.log", maxBytes=1024 * 1024, backupCount=1, encoding="utf8"
)
_stream_handler = logging.StreamHandler(sys.stdout)
_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
_file_handler.setFormatter(_formatter)
_stream_handler.setFormatter(_formatter)
logger.addHandler(_file_handler)
logger.addHandler(_stream_handler)


# ---------------------------------------------------------------------------
# Environment / secrets
# ---------------------------------------------------------------------------
def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        logger.error(f"Required environment variable '{name}' is not set.")
        sys.exit(1)
    return value


SNOWFLAKE_ACCOUNT   = _require_env("SNOWFLAKE_ACCOUNT")    # e.g. xy12345.us-east-1
SNOWFLAKE_USER      = _require_env("SNOWFLAKE_USER")
SNOWFLAKE_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS_DEV")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE", "")
SNOWFLAKE_PRIVATE_KEY_PEM = _require_env("SNOWFLAKE_PRIVATE_KEY_PEM")  # full PEM content as secret

SMTP_SERVER   = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT     = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER     = _require_env("SMTP_USER")       # e.g. reports@yourorg.com
SMTP_PASS     = _require_env("SMTP_PASS")       # app password
EMAIL_FROM    = os.environ.get("EMAIL_FROM", "")  # defaults to SMTP_USER if blank
EMAIL_FROM_NAME = os.environ.get("EMAIL_FROM_NAME", "Coaching Team")


# ---------------------------------------------------------------------------
# Snowflake connection (key-pair auth)
# ---------------------------------------------------------------------------
def get_snowflake_connection():
    pem_bytes = SNOWFLAKE_PRIVATE_KEY_PEM.encode("utf-8")
    private_key = load_pem_private_key(pem_bytes, password=None)

    connect_kwargs = dict(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=private_key,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )
    if SNOWFLAKE_ROLE:
        connect_kwargs["role"] = SNOWFLAKE_ROLE

    return snowflake.connector.connect(**connect_kwargs)


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
FEEDBACK_QUERY = """
WITH qa AS (
  SELECT
      s.ID                         AS submission_id,
      s.CREATED_AT::timestamp_ntz  AS created_at,
      s.FORM_ID::string            AS form_id,
      f.value:"name"::string       AS question_name,
      f.value:"text"::string       AS question_text,
      f.value:"type"::string       AS question_type,
      COALESCE(
        f.value:"prettyFormat"::string,
        f.value:"answer"::string,
        CASE WHEN IS_OBJECT(f.value:"answer") THEN TO_VARCHAR(f.value:"answer") END
      ) AS answer_value
  FROM ANALYTICS_DEV.RAW__JOTFORM_VIEW.SUBMISSIONS s,
       LATERAL FLATTEN(input => s.ANSWERS) f
),
per_submission AS (
  SELECT
      submission_id,
      created_at,
      form_id,
      MAX(IFF(question_name = 'agency',             answer_value, NULL)) AS customer_name,
      MAX(IFF(question_name = 'selectCoach',         answer_value, NULL)) AS coach_name,
      MAX(IFF(question_name = 'name',                answer_value, NULL)) AS member_name,
      MAX(IFF(question_name = 'commentsquestions',   answer_value, NULL)) AS comments,
      MAX(IFF(question_name = 'yourFeedback',        answer_value, NULL)) AS consent_raw,
      MAX(IFF(
        LOWER(question_text) LIKE '%consent to use my testimonial%',
        answer_value, NULL
      )) AS testimonial_consent_raw,
      MAX(IFF(
        LOWER(question_text) LIKE '%my coach cares%' OR question_name IN ('myCoach','typeA7'),
        TRY_TO_NUMBER(REGEXP_SUBSTR(answer_value, '^[0-9]+')),
        NULL
      )) AS rating
  FROM qa
  GROUP BY 1,2,3
),
final AS (
  SELECT
    submission_id,
    created_at,
    DATE_TRUNC('WEEK', created_at) AS week_start,
    customer_name,
    coach_name,
    IFF(consent_raw ILIKE 'Yes%', TRUE, FALSE) AS consent_to_share,
    CASE
      WHEN consent_raw ILIKE 'Yes%' AND LOWER(consent_raw) LIKE '%anonym%' THEN TRUE
      WHEN consent_raw ILIKE 'Yes%' THEN FALSE
      ELSE NULL
    END AS share_anonymized,
    CASE
      WHEN consent_raw ILIKE 'Yes%' AND LOWER(consent_raw) LIKE '%anonym%' THEN NULL
      ELSE member_name
    END AS member_name,
    rating,
    comments,
    consent_raw,
    testimonial_consent_raw
  FROM per_submission
)
SELECT
  f.*,
  d.coach_email
FROM final f
JOIN ANALYTICS_DEV.ANALYTICS_CORE.DIM__COACHES d
  ON f.coach_name = d.coach_name
WHERE f.consent_to_share = TRUE
  AND f.coach_name IS NOT NULL
  AND f.week_start = DATE_TRUNC('WEEK', DATEADD('week', %(week_offset)s, CURRENT_DATE))
  AND (%(coach_filter)s IS NULL OR LOWER(f.coach_name) = LOWER(%(coach_filter)s))
ORDER BY f.created_at DESC
"""


def fetch_feedback(conn, week_offset: int = -1, coach_filter: str | None = None) -> dict[str, dict]:
    """
    Returns a dict keyed by coach_name.

    week_offset: -1 = last week (production default), 0 = current week (test mode)
    coach_filter: if set, only return rows for that coach (case-insensitive)
    """
    cursor = conn.cursor()
    try:
        logger.info(f"Executing feedback query (week_offset={week_offset}, coach={coach_filter or 'all'})…")
        cursor.execute(FEEDBACK_QUERY, {"week_offset": week_offset, "coach_filter": coach_filter})
        columns = [col[0].lower() for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        logger.info(f"Fetched {len(rows)} consented feedback rows.")
    finally:
        cursor.close()

    coaches: dict[str, dict] = {}
    for row in rows:
        name = row["coach_name"]
        if name not in coaches:
            coaches[name] = {
                "coach_name": name,
                "coach_email": row["coach_email"],
                "week_start": row["week_start"],
                "rows": [],
            }
        coaches[name]["rows"].append(row)

    return coaches


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------
def render_report(coach_data: dict) -> str:
    env = Environment(
        loader=FileSystemLoader("."),
        autoescape=True,
    )
    template = env.get_template("report_template.html")

    rows = coach_data["rows"]
    ratings = [r["rating"] for r in rows if r.get("rating") is not None]
    avg_rating = round(sum(ratings) / len(ratings), 1) if ratings else None

    return template.render(
        coach_name=coach_data["coach_name"],
        week_start=coach_data["week_start"].strftime("%B %d, %Y")
            if hasattr(coach_data["week_start"], "strftime")
            else str(coach_data["week_start"]),
        total_responses=len(rows),
        avg_rating=avg_rating,
        max_rating=5,
        feedback_rows=rows,
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    )


# ---------------------------------------------------------------------------
# Email sending
# ---------------------------------------------------------------------------
def send_email(to_email: str, to_name: str, subject: str, html_body: str):
    from_addr = EMAIL_FROM or SMTP_USER
    display_from = f"{EMAIL_FROM_NAME} <{from_addr}>"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = display_from
    msg["To"]      = f"{to_name} <{to_email}>"

    # Plain-text fallback (strip tags crudely — coaches will see HTML anyway)
    plain = "Please view this email in an HTML-capable client to see your feedback report."
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)

    logger.info(f"Email sent to {to_email}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Send weekly coach feedback reports.")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode: query the current week and send to a single override address.",
    )
    parser.add_argument(
        "--coach",
        metavar="NAME",
        help="(Test mode) Coach name to pull the report for (case-insensitive).",
    )
    parser.add_argument(
        "--to",
        metavar="EMAIL",
        help="(Test mode) Email address to send the report to instead of the real coach.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.test:
        if not args.coach or not args.to:
            logger.error("--test requires both --coach and --to. Example:\n"
                         "  python main.py --test --coach 'Jane Smith' --to you@example.com")
            sys.exit(1)
        logger.info(f"TEST MODE — coach: '{args.coach}', sending to: {args.to}")
        week_offset = 0          # current week
        coach_filter = args.coach
    else:
        week_offset = -1         # last week (production default)
        coach_filter = None

    conn = get_snowflake_connection()
    try:
        coaches = fetch_feedback(conn, week_offset=week_offset, coach_filter=coach_filter)
    finally:
        conn.close()

    if not coaches:
        period = "this week" if args.test else "last week"
        logger.info(f"No consented feedback found for {period}{' for coach: ' + args.coach if args.test else ''}. No emails sent.")
        sys.exit(0)

    errors = []
    for coach_name, coach_data in coaches.items():
        try:
            html = render_report(coach_data)
            week_label = coach_data["week_start"].strftime("%b %d, %Y") \
                if hasattr(coach_data["week_start"], "strftime") \
                else str(coach_data["week_start"])
            subject = f"Your Weekly Feedback Summary — Week of {week_label}"
            if args.test:
                subject = f"[TEST] {subject}"

            # In test mode, redirect to the override address
            to_email = args.to if args.test else coach_data["coach_email"]
            to_name  = args.to if args.test else coach_name

            send_email(
                to_email=to_email,
                to_name=to_name,
                subject=subject,
                html_body=html,
            )
        except Exception as exc:
            logger.error(f"Failed to process/send report for {coach_name}: {exc}", exc_info=True)
            errors.append(coach_name)

    if errors:
        logger.error(f"Finished with errors for coaches: {errors}")
        sys.exit(1)
    else:
        logger.info(f"All done. Reports sent to {len(coaches)} coach(es).")
