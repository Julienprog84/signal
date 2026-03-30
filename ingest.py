"""
Signal — Gmail Ingestion Script
Fetches unread newsletters from Gmail, summarizes with Claude Haiku,
updates docs/newsletters.json which powers the dashboard.
"""

import imaplib
import email
import json
import os
import hashlib
import re
from datetime import datetime, timedelta
from email.header import decode_header
from bs4 import BeautifulSoup
import anthropic

# ── Config ──────────────────────────────────────────────────────────────────
GMAIL_ADDRESS = os.environ["GMAIL_ADDRESS"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
DATA_PATH = "docs/newsletters.json"
MAX_DAYS_OLD = 7
MAX_CONTENT_CHARS = 4000
MAX_EMAILS = 20  # Max emails to process per run

# ── Newsletter source mapping ────────────────────────────────────────────────
# Maps sender email/domain to theme metadata
# Add new newsletters here as you subscribe to more
SENDER_MAP = [
    # VC & Deal Flow
    {"match": "fortune.com",               "name": "Term Sheet",              "source": "Fortune / Dan Primack",  "theme": "VC & Deal Flow",      "themeColor": "#E84B2E", "region": "US"},
    {"match": "axios.com",                 "name": "Pro Rata",                "source": "Axios / Dan Primack",    "theme": "VC & Deal Flow",      "themeColor": "#E84B2E", "region": "US"},
    {"match": "pitchbook.com",             "name": "PitchBook",               "source": "PitchBook",              "theme": "VC & Deal Flow",      "themeColor": "#E84B2E", "region": "Global"},
    {"match": "cbinsights.com",            "name": "CB Insights",             "source": "CB Insights",            "theme": "VC & Deal Flow",      "themeColor": "#E84B2E", "region": "Global"},
    # Macro & Strategy
    {"match": "thediff.co",                "name": "The Diff",                "source": "Byrne Hobart",           "theme": "Macro & Finance",     "themeColor": "#2563EB", "region": "US"},
    {"match": "notboring.co",              "name": "Not Boring",              "source": "Packy McCormick",        "theme": "Strategy & Thesis",   "themeColor": "#0891B2", "region": "US"},
    {"match": "generalist.com",            "name": "The Generalist",          "source": "Mario Gabriele",         "theme": "Strategy & Thesis",   "themeColor": "#0891B2", "region": "US"},
    {"match": "exponentialview.co",        "name": "Exponential View",        "source": "Azeem Azhar",            "theme": "Strategy & Thesis",   "themeColor": "#0891B2", "region": "Global"},
    {"match": "ben-evans.com",             "name": "Benedict Evans",          "source": "Benedict Evans",         "theme": "Strategy & Thesis",   "themeColor": "#0891B2", "region": "Global"},
    {"match": "stratechery.com",           "name": "Stratechery",             "source": "Ben Thompson",           "theme": "Strategy & Thesis",   "themeColor": "#0891B2", "region": "Global"},
    # Deep Tech
    {"match": "deeptechnewsletter.com",    "name": "Deep Tech Newsletter",    "source": "Deep Tech Newsletter",   "theme": "Deep Tech & Science", "themeColor": "#7C3AED", "region": "Global"},
    {"match": "defensetechnewsletter",     "name": "Defense Tech Newsletter", "source": "Defense Tech",           "theme": "Deep Tech & Science", "themeColor": "#7C3AED", "region": "US"},
    # China & Global
    {"match": "techbuzzchina.com",         "name": "Tech Buzz China",         "source": "Tech Buzz China",        "theme": "China & Global",      "themeColor": "#DC2626", "region": "China"},
    {"match": "technode.com",              "name": "TechNode",                "source": "TechNode",               "theme": "China & Global",      "themeColor": "#DC2626", "region": "China"},
    {"match": "caixinglobal.com",          "name": "Caixin Global",           "source": "Caixin",                 "theme": "China & Global",      "themeColor": "#DC2626", "region": "China"},
    # Europe
    {"match": "sifted.eu",                 "name": "Sifted",                  "source": "Sifted",                 "theme": "European VC",         "themeColor": "#059669", "region": "Europe"},
    {"match": "maddyness.com",             "name": "Maddyness",               "source": "Maddyness",              "theme": "European VC",         "themeColor": "#059669", "region": "Europe"},
    # Climate & Energy
    {"match": "heatmap.news",              "name": "Heatmap",                 "source": "Heatmap News",           "theme": "Climate & Energy",    "themeColor": "#D97706", "region": "Global"},
    {"match": "volts.wtf",                 "name": "Volts",                   "source": "David Roberts",          "theme": "Climate & Energy",    "themeColor": "#D97706", "region": "Global"},
    {"match": "latitudemedia.com",         "name": "Latitude Media",          "source": "Latitude Media",         "theme": "Climate & Energy",    "themeColor": "#D97706", "region": "Global"},
    # Fintech
    {"match": "fintechbusinessweekly",     "name": "Fintech Business Weekly", "source": "Jason Mikula",           "theme": "Fintech",             "themeColor": "#0D9488", "region": "Global"},
    {"match": "fintechblueprint",          "name": "Fintech Blueprint",       "source": "Lex Sokolin",            "theme": "Fintech",             "themeColor": "#0D9488", "region": "Global"},
    # Substack fallback (many newsletters use substack.com as sender)
    {"match": "substack.com",              "name": "Newsletter",              "source": "Substack",               "theme": "Strategy & Thesis",   "themeColor": "#0891B2", "region": "Global"},
]

def match_sender(from_addr):
    """Match email sender to a known newsletter source."""
    from_addr = from_addr.lower()
    for s in SENDER_MAP:
        if s["match"] in from_addr:
            return s
    return None

def decode_mime_header(value):
    """Decode email header to string."""
    if not value:
        return ""
    parts = decode_header(value)
    decoded = []
    for part, enc in parts:
        if isinstance(part, bytes):
            decoded.append(part.decode(enc or "utf-8", errors="replace"))
        else:
            decoded.append(part)
    return " ".join(decoded)

def extract_text(msg):
    """Extract clean text from email message."""
    text = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/html":
                try:
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or "utf-8"
                    html = payload.decode(charset, errors="replace")
                    soup = BeautifulSoup(html, "html.parser")
                    # Remove navigation, footer, unsubscribe sections
                    for tag in soup(["nav", "footer", "script", "style"]):
                        tag.decompose()
                    text = soup.get_text(separator=" ", strip=True)
                    break
                except Exception:
                    continue
            elif ct == "text/plain" and not text:
                try:
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or "utf-8"
                    text = payload.decode(charset, errors="replace")
                except Exception:
                    continue
    else:
        try:
            payload = msg.get_payload(decode=True)
            charset = msg.get_content_charset() or "utf-8"
            raw = payload.decode(charset, errors="replace")
            if "<html" in raw.lower():
                soup = BeautifulSoup(raw, "html.parser")
                text = soup.get_text(separator=" ", strip=True)
            else:
                text = raw
        except Exception:
            pass

    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:MAX_CONTENT_CHARS]

# Source URL map — fallback homepage per newsletter name
SOURCE_URLS = {
    "Term Sheet": "https://fortune.com/section/term-sheet/",
    "Pro Rata": "https://www.axios.com/newsletters/axios-pro-rata",
    "PitchBook": "https://pitchbook.com/news/newsletters",
    "CB Insights": "https://www.cbinsights.com/newsletter",
    "The Diff": "https://www.thediff.co",
    "Not Boring": "https://www.notboring.co",
    "The Generalist": "https://www.generalist.com",
    "Exponential View": "https://www.exponentialview.co",
    "Benedict Evans": "https://www.ben-evans.com/newsletter",
    "Stratechery": "https://stratechery.com",
    "Deep Tech Newsletter": "https://deeptechnewsletter.com",
    "Defense Tech Newsletter": "https://defensetechnewsletter.curated.co",
    "Tech Buzz China": "https://www.techbuzzchina.com",
    "TechNode": "https://technode.com",
    "Caixin Global": "https://www.caixinglobal.com",
    "Sifted": "https://sifted.eu",
    "Maddyness": "https://www.maddyness.com",
    "Heatmap": "https://heatmap.news",
    "Volts": "https://www.volts.wtf",
    "Latitude Media": "https://www.latitudemedia.com",
    "Fintech Business Weekly": "https://fintechbusinessweekly.substack.com",
    "Fintech Blueprint": "https://www.fintechblueprint.com",
}

def summarize(title, content, theme):
    """Call Claude Haiku to summarize newsletter content.
    Returns None if the email is not substantive editorial content."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""You are an investment intelligence analyst. Analyze this newsletter email and respond in JSON only.

Newsletter: {title}
Theme: {theme}
Content:
{content}

IMPORTANT: First determine if this email contains substantive editorial/investment content worth summarizing.
Reject emails that are: welcome/confirmation emails, GDPR notices, privacy policy updates, unsubscribe confirmations, billing receipts, or any email with no actual editorial content.

Respond with ONLY valid JSON (no markdown, no explanation):
{{
  "is_substantive": true or false,
  "summary": "2-3 sentence summary focused on investment-relevant insights. Be specific about companies, figures, and trends mentioned. Write in clear journalistic prose without jargon.",
  "keyTakeaways": [
    "First sharp, actionable takeaway for a VC/PE investor — written in plain English",
    "Second takeaway — what does this mean for capital allocation?",
    "Third takeaway — macro or sector implication"
  ],
  "readTime": <estimated minutes to read the full article, integer>
}}

If is_substantive is false, still return the full JSON structure but with empty strings for the other fields."""

    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=700,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = message.content[0].text.strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        result = json.loads(raw)

        # Return None if Claude flagged as non-substantive
        if not result.get("is_substantive", True):
            return None

        return result
    except Exception as e:
        print(f"  ⚠ Claude error: {e}")
        return {
            "is_substantive": True,
            "summary": content[:200] + "...",
            "keyTakeaways": ["Unable to generate AI summary — read original"],
            "readTime": 5
        }

def fetch_gmail():
    """Connect to Gmail via IMAP and fetch unread newsletters."""
    print("📬 Connecting to Gmail...")
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
    mail.select("inbox")

    # Search for unread emails from last 7 days
    cutoff = (datetime.now() - timedelta(days=MAX_DAYS_OLD)).strftime("%d-%b-%Y")
    _, data = mail.search(None, f'(UNSEEN SINCE "{cutoff}")')
    email_ids = data[0].split()

    print(f"  Found {len(email_ids)} unread emails")
    emails = []

    for eid in email_ids[-MAX_EMAILS:]:  # Process most recent first
        _, msg_data = mail.fetch(eid, "(RFC822)")
        msg = email.message_from_bytes(msg_data[0][1])

        from_addr = decode_mime_header(msg.get("From", ""))
        subject = decode_mime_header(msg.get("Subject", ""))
        date_str = msg.get("Date", "")

        # Parse date
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(date_str)
            date = dt.strftime("%Y-%m-%d")
        except Exception:
            date = datetime.now().strftime("%Y-%m-%d")

        # Match to known source
        source_meta = match_sender(from_addr)
        if not source_meta:
            print(f"  ↷ Unknown sender, skipping: {from_addr[:50]}")
            continue

        content = extract_text(msg)
        if len(content) < 100:
            print(f"  ↷ Too short, skipping: {subject[:50]}")
            continue

        emails.append({
            "subject": subject,
            "from": from_addr,
            "date": date,
            "content": content,
            "meta": source_meta,
            "email_id": eid.decode()
        })

        # Mark as read
        mail.store(eid, '+FLAGS', '\\Seen')

    mail.logout()
    return emails

def load_existing():
    if os.path.exists(DATA_PATH):
        with open(DATA_PATH) as f:
            return json.load(f)
    return {"lastUpdated": None, "newsletters": [], "themes": []}

def save_data(data):
    os.makedirs("docs", exist_ok=True)
    data["lastUpdated"] = datetime.now().isoformat() + "Z"
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✓ Saved {len(data['newsletters'])} newsletters to {DATA_PATH}")

def make_id(subject, date):
    return hashlib.md5(f"{subject}{date}".encode()).hexdigest()[:8]

def ingest():
    print("🔄 Signal — Starting ingestion...")
    existing = load_existing()
    existing_ids = {n["id"] for n in existing["newsletters"]}

    emails = fetch_gmail()
    new_items = []

    for e in emails:
        item_id = make_id(e["subject"], e["date"])
        if item_id in existing_ids:
            print(f"  ↷ Already processed: {e['subject'][:50]}")
            continue

        m = e["meta"]
        print(f"  ✦ Summarizing: {e['subject'][:60]}")

        ai = summarize(
            title=e["subject"],
            content=e["content"],
            theme=m["theme"]
        )

        if ai is None:
            print(f"  ✗ Skipped (not substantive): {e['subject'][:60]}")
            continue

        source_url = SOURCE_URLS.get(m["name"], "")

        new_items.append({
            "id": item_id,
            "title": m["name"],
            "source": m["source"],
            "date": e["date"],
            "theme": m["theme"],
            "themeColor": m["themeColor"],
            "region": m["region"],
            "summary": ai["summary"],
            "keyTakeaways": ai["keyTakeaways"],
            "url": source_url,
            "readTime": ai.get("readTime", 5),
            "originalTitle": e["subject"]
        })

    # Merge and cull old items
    cutoff = (datetime.now() - timedelta(days=MAX_DAYS_OLD)).strftime("%Y-%m-%d")
    combined = new_items + [n for n in existing["newsletters"] if n["date"] >= cutoff]
    combined.sort(key=lambda x: x["date"], reverse=True)

    # Rebuild themes
    theme_map = {}
    for n in combined:
        if n["theme"] not in theme_map:
            theme_map[n["theme"]] = {"name": n["theme"], "color": n["themeColor"], "count": 0}
        theme_map[n["theme"]]["count"] += 1

    save_data({"newsletters": combined, "themes": list(theme_map.values())})
    print(f"\n✅ Done. {len(new_items)} new items processed.")

if __name__ == "__main__":
    ingest()
