# coach-feedback-weekly

A GitHub Actions pipeline that queries Snowflake for the previous week's member feedback, generates a styled HTML report per coach, and emails each coach their results every Monday morning.

---

## How it works

1. **Query** — `main.py` connects to Snowflake using key-pair authentication and runs a CTE query against `ANALYTICS_PROD.RAW__JOTFORM_VIEW.SUBMISSIONS`. It flattens JotForm submission answers, pivots them into one row per submission, applies consent filtering (`consent_to_share = TRUE`), and joins against `ANALYTICS_PROD.ANALYTICS_CORE.DIM__COACHES` to resolve each coach's email address.

2. **Group** — Results are grouped by coach. For each coach, the script computes the total response count and average rating.

3. **Render** — A Jinja2 HTML template (`report_template.html`) is rendered per coach, producing a self-contained styled email with a summary stats bar and individual feedback cards. Anonymized submissions show "Anonymous Member" instead of the member's name.

4. **Send** — Each report is sent to the coach's email via SMTP (STARTTLS on port 587, defaulting to Gmail). In test mode, all emails are redirected to a single override address.

5. **Log** — All activity is written to `status.log` (rotating, 1 MB max) and stdout. The GitHub Action commits the updated log back to `main` after each run.

---

## Project structure

```
├── main.py                        # Main script: query, render, send
├── report_template.html           # Jinja2 HTML email template
├── requirements.txt               # Python dependencies
├── status.log                     # Rotating run log (auto-committed by CI)
└── .github/
    └── workflows/
        └── actions.yml            # GitHub Actions workflow
```

---

## Configuration

### GitHub Secrets (Settings → Secrets and variables → Actions → Secrets)

| Secret | Required | Description |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | ✅ | Snowflake account identifier, e.g. `xy12345.us-east-1` |
| `SNOWFLAKE_USER` | ✅ | Snowflake username |
| `SNOWFLAKE_PRIVATE_KEY_PEM` | ✅ | Full contents of your `.p8` private key file (including `-----BEGIN...-----END` lines) |
| `SMTP_USER` | ✅ | Sending email address, e.g. `reports@yourorg.com` |
| `SMTP_PASS` | ✅ | App password for the sending account (not your login password) |

### GitHub Variables (Settings → Secrets and variables → Actions → Variables)

| Variable | Required | Description |
|---|---|---|
| `SNOWFLAKE_WAREHOUSE` | ✅ | Snowflake warehouse name |
| `SNOWFLAKE_ROLE` | ✅ | Snowflake role |

### Optional (have sensible defaults, set as secrets only if you need to override)

| Name | Default | Description |
|---|---|---|
| `SNOWFLAKE_DATABASE` | `ANALYTICS_PROD` | Snowflake database |
| `SMTP_SERVER` | `smtp.gmail.com` | SMTP hostname |
| `SMTP_PORT` | `587` | SMTP port |
| `EMAIL_FROM` | Same as `SMTP_USER` | From address if different from sending account |
| `EMAIL_FROM_NAME` | `Coaching Team` | Display name shown in From field |

---

## Usage

### Production

The workflow runs automatically **every Monday at 07:00 UTC**, covering the full prior Monday–Sunday week. You can also trigger it manually:

1. Go to **Actions** → **Weekly Coach Feedback Reports**
2. Click **Run workflow**

### Testing — send one coach's report to yourself

Run locally with the `--test` flag. This queries the **current** week (not last week) and redirects the email to the address you specify:

```bash
python main.py --test --coach "Jane Smith" --to you@example.com
```

- `--coach` — coach name to pull feedback for (case-insensitive, must match the name in Snowflake)
- `--to` — email address to receive the report instead of the real coach
- The subject line will be prefixed with `[TEST]` so it's easy to identify

---

## Changing the report format

Edit **`report_template.html`**. It's a standard Jinja2 template with inline CSS (required for email client compatibility).

Available template variables:

| Variable | Type | Description |
|---|---|---|
| `coach_name` | `str` | Coach's full name |
| `week_start` | `str` | Formatted week start date, e.g. `February 10, 2026` |
| `total_responses` | `int` | Number of feedback submissions |
| `avg_rating` | `float \| None` | Average rating out of 5, or `None` if no ratings |
| `max_rating` | `int` | Always `5` |
| `feedback_rows` | `list[dict]` | One dict per submission — see fields below |
| `generated_at` | `str` | UTC timestamp of when the report was generated |

Each item in `feedback_rows` has:

| Field | Description |
|---|---|
| `member_name` | Member's name, or `None` if anonymized |
| `share_anonymized` | `True` if the member requested anonymity |
| `customer_name` | Agency / customer the member belongs to |
| `rating` | Numeric rating (1–5), or `None` |
| `comments` | Free-text feedback, or `None` |
| `created_at` | Submission timestamp |

---

## Changing when reports are sent

Edit the cron expression in `.github/workflows/actions.yml`:

```yaml
on:
  schedule:
    - cron: '0 7 * * 1'  # Every Monday at 07:00 UTC
```

[crontab.guru](https://crontab.guru) is useful for building cron expressions. Note that GitHub Actions schedules run in UTC.
